// TODO temp new package name
package org.apache.hadoop.hbase.regionserver.memstore.replication.v2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionAdminServiceCallable;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.regionserver.memstore.replication.MemstoreReplicationEntry;
import org.apache.hadoop.hbase.regionserver.memstore.replication.PipelineException;
import org.apache.hadoop.hbase.regionserver.memstore.replication.SimpleMemstoreReplicator;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MemstoreReplicaProtos.ReplicateMemstoreResponse;

// This is a per Region instance 
//TODO better name
/**
 * Notes : How the different data structures like regionLocations, badReplicas, pipeline work
 * altogether. regionLocations - Keep the replica Locations in an replicaId indexed array.
 *
 * badReplicas - This is used by primary region alone. In other replica regions, this will be a
 * never used data structure. When a write to one of the replica is failed, the primary will mark it
 * as BAD and also ask HM to mark the same in the META table. Even before asking the HM, that
 * replica will be added to this set. Also when the down replicas are coming back, it would ask the
 * primary to do flush. Then itself it will be removed from badRepicas. This Set is been used for
 * the pipeline creation. Once the BAD replica comes back and asks for flush, we have to make the
 * write to go to those regions. The flush would have flushed cells till then only. The replica will
 * be marked back to GOOD in META only after the flush is finished and the replica updated its files
 * list so that it is having all data upto date. In other words, the status in META denotes which
 * all replicas are having upto date data and this Set just tells primary that which all replicas to
 * be considered for replicating data.
 *
 * badReplicasInMeta - This includes the replicas which are actually marked as BAD in META. This is
 * also a primary region side only Set.
 *
 * pipeline - This includes the indices of all active replicas at any moment, in the order of its
 * replica id. The replication of memstore writes to happen to all these. This is also a primary
 * only Set. The pipeline info will be passed the PB requests to other replicas. So each of the
 * replica use that info in the request to decide to replicate to which next replica from it.
 *
 * badCountToBeCommittedInMeta - This says whether there are some BAD replicas still which are not
 * yet marked as BAD in META. When we have such cases, all the writes during that period will be
 * immediately failed. (Will be retried from client as the Exception is not of
 * DoNotRetryIOException) This is because of the following reason. The src of truth abt the health
 * is at META. If we allow more writes when we dont replicate to a replica (as that is BAD as per
 * badReplicas set) and make them success(There may be enough number of min replicas still), we are
 * not having correct status of upto date data in META. Before making the META entry, the HM may
 * die. Also then another HM comes up and then this primary die. Still the META do not say the real
 * BAD replica as BAD. So it can so happen that this got promoted as new primary and we will have
 * data loss!
 *
 * regionLocations - Holds the location for each of the replica regions. This is in use in every
 * replica regions. For the first time, this is loaded from META read. Later when any region
 * decides replica(s) as failed (as it could not replicate to there), it calls processBadReplicas
 * and that will reset the corresponding entry in regionLocations as null. Pls see only primary will
 * change the pipeline. Later when region join back the primary, it will change the pipeline to
 * include this also and pass that to other replicas also in pipeline info. When the replication
 * process can not see the corresponding location in regionLocations for the given replica id, it
 * will reload it from META.
 */
public class RegionReplicaReplicator {
  private static final long UNSET = -1L;
  private final Configuration conf;
  private HRegionInfo curRegion;
  private final int minNonPrimaryWriteReplicas;
  private volatile HRegionLocation[] regionLocations;
  private volatile List<MemstoreReplicationEntry> entryBuffer = new ArrayList<>();
  private volatile long nextSeq = 1;
  private volatile long curMaxConsumedSeq = 0;
  private int replicationThreadIndex;
  // Those replicas whose health is marked as BAD already in this primary
  private Set<Integer> badReplicas;
  // This is a subset of badReplicas. We will add to below DS only after the region is marked as BAD
  // in META where as we will add to above immediately after we see a replica as out of data
  // consistency as one of the write did not reach there
  private Set<Integer> badReplicasInMeta;
  private Set<Integer> pipeline;
  private ReadWriteLock lock = new ReentrantReadWriteLock();
  private AtomicInteger badCountToBeCommittedInMeta = new AtomicInteger(0);

  private volatile long badReplicaInProgressTs = UNSET;
  private static final Log LOG = LogFactory.getLog(RegionReplicaReplicator.class);

  public RegionReplicaReplicator(Configuration conf, HRegionInfo currentRegion, int minWriteReplicas,
       int replicationThreadIndex) {
    this.conf = conf;
    this.curRegion = currentRegion;
    this.minNonPrimaryWriteReplicas = minWriteReplicas - 1;
    this.replicationThreadIndex = replicationThreadIndex;
    if (RegionReplicaUtil.isDefaultReplica(this.curRegion)) {
      badReplicas = new HashSet<>();
      badReplicasInMeta = new HashSet<>();
    }
  }

  public CompletableFuture<ReplicateMemstoreResponse> append(MemstoreReplicationEntry entry)
      throws IOException {
    CompletableFuture<ReplicateMemstoreResponse> future = new CompletableFuture<>();
    // Seems no way to avoid this sync
    synchronized (this) {
      entry.attachFuture(future, nextSeq++);
      this.entryBuffer.add(entry);
    }
    return future;
  }

  /**
   * @param minSeq
   *          The min sequence number we expect to return. If that is not there (already retrieved
   *          and processed by some one else), we will return null.
   */
  public synchronized List<MemstoreReplicationEntry> pullEntries(long minSeq) {
    // TODO : some problem here.  Fix !!  This is a very imp fix. What is the issue (Anoop)
    if (this.curMaxConsumedSeq >= minSeq) {
      return null;
    }
    List<MemstoreReplicationEntry> local = this.entryBuffer;
    this.entryBuffer = new ArrayList<>();
    this.curMaxConsumedSeq = this.nextSeq - 1;
    return local;
  }

  public HRegionLocation getRegionLocation(int replicaId) {
    // TODO this location may be null also as marked by the processBadReplicas(). Reload then.
    // TODO after a new replica is added at runtime, (Table alter) and if this is a secondary
    // replica, it can so happen that we dont have this replicaId itself in the array. Handle so as
    // not to get Array Index Out of Bounds.
    return this.regionLocations[replicaId];
  }

  public int getCurRegionReplicaId() {
    return this.curRegion.getReplicaId();
  }

  public TableName getTableName() {
    return this.curRegion.getTable();
  }

  public HRegionInfo getRegionInfo() {
    return this.curRegion;
  }

  public int getReplicationThreadIndex() {
    return this.replicationThreadIndex;
  }

  public List<Integer> processBadReplicas(List<Integer> replicas) {
    List<Integer> addedReplicas = new ArrayList<>(replicas.size());
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      if (RegionReplicaUtil.isDefaultReplica(curRegion)) {
        for (Integer replica : replicas) {
          if (!this.badReplicas.contains(replica)) {
            addedReplicas.add(replica);
            this.badReplicas.add(replica);
            this.pipeline.remove(replica);
          }
        }
        this.badCountToBeCommittedInMeta.addAndGet(addedReplicas.size());
      }
      // For any of the region replica, we have to update the RegionLocations. These passed replicas
      // are any way treated as BAD from now on. Later when we mark again as good, we might reload
      // the location
      for (Integer replica : replicas) {
        this.regionLocations[replica] = null;
      }
    } finally {
      lock.unlock();
    }
    return addedReplicas;
  }

  // TODO to have an API to add a new Replica into the list. This can happen when table is altered.
  // The newly opened Region will ask the primary to flush so as to make itself up to date. Primary
  // has to find out that this is a fresh replica and add here.

  /**
   * @param replicas
   * @return True if no more BAD replicas are yet to be added to META.
   */
  public void onReplicasBadInMeta(List<Integer> replicas) {
    // This should be called only when this is Primary Region
    assert RegionReplicaUtil.isDefaultReplica(curRegion);
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      for (Integer replica : replicas) {
        this.badReplicasInMeta.add(replica);
      }
      this.badCountToBeCommittedInMeta.addAndGet(-1 * replicas.size());
    } finally {
      lock.unlock();
    }
  }

  public boolean isReplicasBadInMeta(List<Integer> replicas) {
    Lock lock = this.lock.readLock();
    lock.lock();
    try {
      for (Integer replica : replicas) {
        if (!this.badReplicasInMeta.contains(replica)) return false;
      }
      return true;
    } finally {
      lock.unlock();
    }
  }
  
  public boolean removeFromBadReplicas(Integer replica) throws PipelineException {
    // This should be called only when this is Primary Region
    assert RegionReplicaUtil.isDefaultReplica(curRegion);
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      // when a replica region opens it makes a call for primary region flush. 
      // there is no pipeline at that point of time. This Null checks helps avoid NPE
      if (pipeline != null) {
        this.pipeline.add(replica);
        // In a region opening sequence (on create table), there is a chance that replica 2
        // is opened first and then the replica 1 is opened. So when replica 2 is opened
        // by the time the region location is updated with primary replica server and replica 2's
        // server. When the replica 1 is opened we don update the region location but since
        // the we add 1 to the pipeline we have a mismatch between pipeline and region location.
        // so we check if the region locations and the pipeline matches and if not we update the region location
        //This is a temp fix only. Need to understand all other cases where a region opening fails and how we deal with it.
        if (!allRegionLocationsLoaded(new ArrayList<Integer>(this.pipeline))) {
          loadRegionLocations(null);
        }
        return this.badReplicas.remove(replica);
      }
      return true;
    } finally {
      lock.unlock();
    }
  }

  // TODO to be used
  public boolean removeFromBadReplicasInMeta(Integer replica) {
    // This should be called only when this is Primary Region
    assert RegionReplicaUtil.isDefaultReplica(curRegion);
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      return this.badReplicasInMeta.remove(replica);
    } finally {
      lock.unlock();
    }
  }

  // TODO - We should not give back any replicas for which state in META is still bad. There is a
  // time gap btw the Good state set here and in META. So that extra check is needed. Once we get
  // the flush request from a replica (after it is up again), then itself we will add that replica
  // into the pipeline. This is not yet ready for the reads. State is META is any way BAD still
  public List<Integer> getCurrentPiplineForReads() {
    Lock lock = this.lock.readLock();
    lock.lock();
    try {
      if (pipeline != null) {
        return new ArrayList<>(this.pipeline);
      } else {
        return null;
      }
    } finally {
      lock.unlock();
    }
  }
  
  public List<Integer> createPipeline() throws PipelineException {
    // This should be called only when this is Primary Region
    assert RegionReplicaUtil.isDefaultReplica(curRegion);
    if (this.regionLocations == null) {
      loadRegionLocations(null);
    }
    Lock lock = this.lock.readLock();
    lock.lock();
    // When there are some pending updates to META for making replica region(s) as BAD, we should
    // not allow writes to continue. The pipeline wont have those replicas any way and while the
    // META is not yet updated, more writes might get committed. There is chance that this META
    // update might not happen before this primary itself going down. Then META says a real BAD
    // replica as good and that might get selected as new primary. Then we might loose some data!
    // How ever still there is a chance that before this count is updated we will have pending writes already
    // in process. That will still go on. So any temp glitch will make writes successful.
    if (this.badCountToBeCommittedInMeta.get() != 0) {
      throw new PipelineException();
    }
    // Early out. Already there are not enough replicas for making the write as successful. Why to
    // continue then? 
    if (this.pipeline.size() < this.minNonPrimaryWriteReplicas) {
      throw new PipelineException();
    }
    try {
      return new ArrayList<>(this.pipeline);
    } finally {
      lock.unlock();
    }
  }

  public List<Integer> verifyPipeline(List<Integer> requestPipeline) throws PipelineException {
    // This should be called only when this is NOT a Primary Region
    assert !RegionReplicaUtil.isDefaultReplica(curRegion);
    if (this.regionLocations == null || !allRegionLocationsLoaded(requestPipeline)) {
      loadRegionLocations(requestPipeline);
    }
    // TODO no extra verify here. We can do the case of new replicas added as part of Table alter
    // and all here.
    return requestPipeline;
  }

  // Compares the region location and pipeline and ensures that the pipeline and regoin location are the same.
  // this is needed on region open sequence where the region locations could be updated even before the replicas
  // are opened
  private boolean allRegionLocationsLoaded(List<Integer> requestPipeline) {
    // make this more easier and avoid these loops. Better to have one more ds having the non default replicas
    // fetched as part of region locations
    for (HRegionLocation loc : this.regionLocations) {
      if (loc != null && loc.getServerName() != null) {
        int replicaId = loc.getRegionInfo().getReplicaId();
        if ((!RegionReplicaUtil.isDefaultReplica(replicaId))
            && (!requestPipeline.contains(replicaId))) {
          LOG.info("The requested pipeline " + requestPipeline + " does not have the replica id "
              + replicaId);
          resetRegionLocations();
          return false;
        }
      }
    }
    // check both ways
    for (int requestReplicaId : requestPipeline) {
      boolean found = false;
      for (HRegionLocation loc : this.regionLocations) {
        if (loc != null && loc.getServerName() != null) {
          int replicaId = loc.getRegionInfo().getReplicaId();
          if ((!RegionReplicaUtil.isDefaultReplica(replicaId)) && requestReplicaId == replicaId) {
            found = true;
            break;
          }
        }
      }
      if (!found) {
        LOG.info("The region location pipeline does not have the requested replica id "
            + requestReplicaId);
        resetRegionLocations();
        return false;
      }
    }
    return true;
  }

  private void resetRegionLocations() {
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      LOG.info("resetting region locations "+this.getRegionInfo());
      this.regionLocations = null;
    } finally {
      lock.unlock();
    }
  }

  private void loadRegionLocations(List<Integer> requestPipeline) throws PipelineException {
    // update the region locations here.
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      if (this.regionLocations == null) {
        RegionLocations regionLocations;
        // Every time reload will create new connection! That may be ok. Why to retain connection
        // that too one per region
        // TODO we can reset some of the thread# related stuff in this conf for connection creation.
        // Any way we know this will be used by single thread only and just once.
        try (ClusterConnection connection = (ClusterConnection) ConnectionFactory
            .createConnection(this.conf)) {
          // never use cache here
          regionLocations = RegionAdminServiceCallable.getRegionLocations(connection,
              this.getRegionInfo().getTable(), this.getRegionInfo().getStartKey(), false,
              this.getRegionInfo().getReplicaId());
        } catch (IOException e) {
          throw new PipelineException(e);
        }
        this.regionLocations = regionLocations.getRegionLocations();
      }
      this.pipeline = new TreeSet<>();
      for (HRegionLocation loc : this.regionLocations) {
        if (loc != null && loc.getServerName() != null) {
          int replicaId = loc.getRegionInfo().getReplicaId();
          if (!RegionReplicaUtil.isDefaultReplica(replicaId)) {
            this.pipeline.add(replicaId);
          }
        }
      }
      LOG.debug("The pipeline created has " + this.pipeline.size() + " " + pipeline + " "
          + this.getRegionInfo());
    } finally {
      lock.unlock();
    }
  }

  public void convertAsPrimaryRegion(HRegionInfo primaryRegion) {
    Lock lock = this.lock.writeLock();
    lock.lock();
    assert !(RegionReplicaUtil.isDefaultReplica(this.curRegion));
    assert RegionReplicaUtil.isDefaultReplica(primaryRegion);
    try {
      this.curRegion = primaryRegion;
      this.badReplicas = new HashSet<>();
      this.badReplicasInMeta = new HashSet<>();
      this.regionLocations = null;// Lets reload location from META. Any way we have done changes to
                                  // the region replica ids.
    } finally {
      lock.unlock();
    }
  }

  public synchronized void finishBadHealthProcessing() {
    assert !(RegionReplicaUtil.isDefaultReplica(this.curRegion));
    this.badReplicaInProgressTs = UNSET;
  }

  public synchronized boolean shouldProcessBadStatus(long ts) {
    assert !(RegionReplicaUtil.isDefaultReplica(this.curRegion));
    if (ts > this.badReplicaInProgressTs) {
      this.badReplicaInProgressTs = ts;
      return true;
    }
    return false;
  }
}
