/**
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.mnemonic.ChunkBuffer;
import org.apache.mnemonic.DurableChunk;
import org.apache.mnemonic.NonVolatileMemAllocator;

/**
 * An implementation of Durable chunk
 */
@InterfaceAudience.Private
public class DurableSlicedChunk extends Chunk {

  private DurableChunk<NonVolatileMemAllocator> durableChunk;
  private long offset;
  private ChunkBuffer chunkBuffer;

  public DurableSlicedChunk(int id, DurableChunk<NonVolatileMemAllocator> durableBigChunk,
      long offset, int size) {
    super(size, id, true);// Durable chunks are always created out of pool.
    this.offset = offset;
    this.durableChunk = durableBigChunk;
  }

  @Override
  void allocateDataBuffer() {
    if (data == null) {
      // fill the data here
      // this causes NPE
      // createBuffer.cancelAutoReclaim();
      chunkBuffer = durableChunk.getChunkBuffer(offset, size);
      data = chunkBuffer.get();
      data.putInt(0, this.getId());
    }
  }

  void persist() {
    // Just added everthing here. TODO : Know what is the difference
    this.chunkBuffer.sync();
    this.chunkBuffer.flush();
    this.chunkBuffer.persist();
  }
}
