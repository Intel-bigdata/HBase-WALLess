/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.memstore.replication;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.hbase.Cell;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MemstoreReplicaProtos.ReplicateMemstoreRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MemstoreReplicaProtos.ReplicateMemstoreResponse;

@InterfaceAudience.Private
public interface MemstoreReplicator {
  
  /**
   * Called by primary Region to replicate to its replicas. 
   */
  ReplicateMemstoreResponse replicate(MemstoreReplicationKey memstoreReplicationKey,
      MemstoreEdits memstoreEdits, RegionReplicaReplicator regionReplicaReplicator)
      throws IOException;

  /**
   * Called by a replica region to replicate to its next replicas. Note that we get the
   * ReplicateMemstoreRequest request directly here not the Key and Edits as in above API.
   */
  ReplicateMemstoreResponse replicate(ReplicateMemstoreRequest request, List<Cell> allCells,
      RegionReplicaReplicator regionReplicaReplicator) throws IOException;

  //TODO : Shall we return our own CompletedFuture here and wait on the Future explicitly??
  /**
   * An async way of replicate to replicas. Used to pass the Meta cells like the flush markers and
   * compaction markers. This is called by primary only. In a replica region, to replicate to itse
   * next replicas, we can make use of the API which takes the ReplicateMemstoreRequest itself. Any
   * way we want the primary region only should NOT wait on a sync call for the op (like
   * flush/compaction)
   */
  CompletableFuture<ReplicateMemstoreResponse> replicateAsync(
      MemstoreReplicationKey memstoreReplicationKey, MemstoreEdits memstoreEdits,
      RegionReplicaReplicator regionReplicaReplicator) throws IOException;

  /**
   * Picks up the next replication thread available when requested by the caller
   * @return the next available replication thread
   */
  public int getNextReplicationThread();

  public long getReplicationTimeout();
}
