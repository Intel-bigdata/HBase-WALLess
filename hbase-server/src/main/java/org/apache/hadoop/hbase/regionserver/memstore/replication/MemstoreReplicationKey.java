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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class represents the region info, table name for the memstore edits that represents a
 * transaction - TODO : Merge this and MemstoreEdits to one class?
 */
@InterfaceAudience.Private
public class MemstoreReplicationKey {
  /**
   */
  public static final long NO_SEQUENCE_ID = -1;
  private byte [] encodedRegionName;

  /**
   * SequenceId for this edit. Set post-construction at write-to-WAL time. Until then it is
   * NO_SEQUENCE_ID. Change it so multiple threads can read it -- e.g. access is synchronized.
   */
  private long sequenceId;

  //Is it right to add here??
  private final int replicasOffered;

  // TODO : Support compression, nonceGroup and nonces, inter cluster replication
  public MemstoreReplicationKey(byte[] encodedRegionNameInBytes, int replicasOffered) {
    this(encodedRegionNameInBytes, NO_SEQUENCE_ID, replicasOffered);
  }

  // TODO : Support compression, nonceGroup and nonces, inter cluster replication
  public MemstoreReplicationKey(byte[] encodedRegionName, long sequenceId, int replicasOffered) {
    this.encodedRegionName = encodedRegionName;
    this.sequenceId = sequenceId;
    this.replicasOffered = replicasOffered;
  }

  public byte[] getEncodedRegionName() {
    return this.encodedRegionName;
  }

  public void setSequenceId(long seqId) {
    this.sequenceId = seqId;
  }
  
  public long getSequenceId() {
    return this.sequenceId;
  }

  /**
   * Drop this instance's region name byte array and instead
   * hold a reference to the provided region name. This is not
   * meant to be a general purpose setter - it's only used
   * to collapse references to conserve memory.
   */
  void internEncodedRegionName(byte []encodedRegionName) {
    // We should not use this as a setter - only to swap
    // in a new reference to the same table name.
    assert Bytes.equals(this.encodedRegionName, encodedRegionName);
    this.encodedRegionName = encodedRegionName;
  }
  
  public int getReplicasOffered() {
    return this.replicasOffered;
  }
  // Add proto file for this. PD serDe methods to be added
}
