package org.apache.hadoop.hbase.regionserver.memstore.replication.v2;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionAdminServiceCallable;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ReplicationProtbufUtil;
import org.apache.hadoop.hbase.regionserver.memstore.replication.MemstoreReplicationEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Pair;

// Singleton instance per RS
public class MemstoreReplicationService {

  private final Configuration conf;
  private final BlockingQueue<RegionReplicaReplicator> regionQueue = new LinkedBlockingQueue<>();
  private ClusterConnection connection;
  private final int operationTimeout;
  private final ReplicationThread[] replicationThreads;

  private MemstoreReplicationService(Configuration conf) {
    this.conf = new Configuration(conf);
    // Adjusting the client retries number. This defaults to 31. (Multiplied by 10?)
    // The more this retries, the more latency we will have when we have some region replica write
    // fails. Adding a new config may be needed. As of now just making this to 2. And the multiplier to 1.
    this.conf.setInt("hbase.client.serverside.retries.multiplier", 1);
    this.conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);

    // TODO : Better math considering Regions count also? As per the cur parallel model, this is enough
    int numWriterThreads = conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT,
        HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT);
    this.replicationThreads = new ReplicationThread[numWriterThreads];
    for (int i = 0; i < numWriterThreads; i++) {
      this.replicationThreads[i] = new ReplicationThread();
      this.replicationThreads[i].start();
    }
    //controller = new PipelineController();

    // use the regular RPC timeout for replica replication RPC's
    this.operationTimeout = conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
        HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
    try {
      this.connection = (ClusterConnection) ConnectionFactory.createConnection(this.conf);
    } catch (IOException ex) {
    }
  }

  public void stop() {
    for (ReplicationThread thread : this.replicationThreads) {
      thread.stop();
    }
  }
  
  private class ReplicationThread extends HasThread {
    
    private volatile boolean closed = false;
    // TODO may be these factory  can be at Top class level.
    private final RpcControllerFactory rpcControllerFactory;
    private final RpcRetryingCallerFactory rpcRetryingCallerFactory;
    
    public ReplicationThread() {
      this.rpcRetryingCallerFactory = RpcRetryingCallerFactory
          .instantiate(connection.getConfiguration());
      this.rpcControllerFactory = RpcControllerFactory.instantiate(connection.getConfiguration());
    }
    
    @Override
    public void run() {
      while (!this.closed) {
        try {
          RegionReplicaReplicator replicator = regionQueue.take();// TODO Check whether this call
                                                                  // will make the thread under wait
                                                                  // or whether consume CPU
          replicate(replicator);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    private ReplicateWALEntryResponse replicate(RegionReplicaReplicator replicator) {
      // TODO we need a new ReplicateWALEntryResponse from where which we can know how many
      // success replicas are there.
      ReplicateWALEntryResponse response = null;
      int curRegionReplicaId = replicator.getCurRegionReplicaId();
      // The write pipeline for replication will always be R1 -> R2 ->.. Rn
      // When there is a failure for any node, the current replica will try with its next and so on
      // Replica ids are like 0, 1, 2...
      for (int i = 1; i < (replicator.getReplicasCount() - curRegionReplicaId); i++) {
        // Getting the location of the next Region Replica (in pipeline)
        HRegionLocation nextRegionLocation = replicator.getRegionLocation(curRegionReplicaId + i);
        if (nextRegionLocation != null) {
          RegionReplicaReplayCallable callable = new RegionReplicaReplayCallable(connection,
              rpcControllerFactory, replicator.getTableName(), nextRegionLocation,
              replicator.getRegionInfo(), null, replicator.pullEntries());
          // Passing row as null is ok as we already know the region location. This row wont be used
          // at all.
          try {
            response = rpcRetryingCallerFactory.<ReplicateWALEntryResponse>newCaller()
                .callWithRetries(callable, operationTimeout);
            return response;// Break the loop. The successful next replica will write to its next
          } catch (IOException | RuntimeException e) {
            // TODO
            // This data was not replicated to given replica means it is in bad state. We have to
            // mark same in META table. Need an RPC call to master for that. Only master should talk
            // to META table. Need add new PB based RPC call.
            // To have a row specific lock here so that only one RPC will go from here to HM. There
            // may be other parallel handlers also trying to write to that replica.
            e.printStackTrace();
          }
        }
      }
      return null;
    }

    public void stop() {
      this.closed = true;
      this.interrupt();
    }
  }

  private static class RegionReplicaReplayCallable
      extends RegionAdminServiceCallable<ReplicateWALEntryResponse> {
    private final List<MemstoreReplicationEntry> entries;
    private final byte[] initialEncodedRegionName;

    public RegionReplicaReplayCallable(ClusterConnection connection,
        RpcControllerFactory rpcControllerFactory, TableName tableName, HRegionLocation location,
        HRegionInfo regionInfo, byte[] row, List<MemstoreReplicationEntry> entries) {
      super(connection, rpcControllerFactory, location, tableName, row, regionInfo.getReplicaId());
      this.entries = entries;
      this.initialEncodedRegionName = regionInfo.getEncodedNameAsBytes();
    }

    public ReplicateWALEntryResponse call(HBaseRpcController controller) throws Exception {
      // Check whether we should still replay this entry. If the regions are changed, or the
      // entry is not coming form the primary region, filter it out because we do not need it.
      // Regions can change because of (1) region split (2) region merge (3) table recreated
      boolean skip = false;
      if (!Bytes.equals(location.getRegionInfo().getEncodedNameAsBytes(),
          initialEncodedRegionName)) {
        skip = true;
      }
      if (!this.entries.isEmpty() && !skip) {
        MemstoreReplicationEntry[] entriesArray = new MemstoreReplicationEntry[this.entries.size()];
        entriesArray = this.entries.toArray(entriesArray);
        // set the region name for the target region replica
        Pair<AdminProtos.ReplicateWALEntryRequest, CellScanner> p = ReplicationProtbufUtil
            .buildReplicateMemstoreEntryRequest(entriesArray,
                location.getRegionInfo().getEncodedNameAsBytes(), null, null, null);
        controller.setCellScanner(p.getSecond());
        return stub.replay(controller, p.getFirst());
      }
      return ReplicateWALEntryResponse.newBuilder().build();
    }
  }
}
