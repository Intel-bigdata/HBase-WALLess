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

import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.mnemonic.DurableChunk;
import org.apache.mnemonic.NonVolatileMemAllocator;
import org.apache.mnemonic.Utils;

@InterfaceAudience.Private
public class DurableChunkCreator extends ChunkCreator {

  private static final Log LOG = LogFactory.getLog(DurableChunkCreator.class);

  private DurableChunk<NonVolatileMemAllocator> durableBigChunk;
  // Offset to track the allocation inside the bigChunk.
  private AtomicLong offset = new AtomicLong(0);

  DurableChunkCreator(int chunkSize, long globalMemStoreSize, float poolSizePercentage,
      String durablePath) {
    super(chunkSize, true);
    // Do validation. but for now creating max sized allocator
    // As per Gary, pmalloc works with any size and pmem is not storage and space efficient
    NonVolatileMemAllocator allocator = new NonVolatileMemAllocator(
        Utils.getNonVolatileMemoryAllocatorService("pmem"),
        (long) ((globalMemStoreSize * poolSizePercentage + (2048l))),
        durablePath, true);
    // This does not work with > 15G
    durableBigChunk = allocator.createChunk((long)((globalMemStoreSize * poolSizePercentage)));
    if (durableBigChunk == null) {
      throw new RuntimeException("Not able to create a durable chunk");
    }
  }

  protected void initializePool(long globalMemStoreSize, float poolSizePercentage,
      float initialCountPercentage, HeapMemoryManager heapMemoryManager) {
    if (poolSizePercentage != 1.0 || initialCountPercentage != 1.0) {
      // When Durable chunks in place, we need to have the entire global memstore size has to be
      // from pool.
      LOG.warn("When Durable chunks in place, we need to have the entire global memstore size has"
          + " to be from pool");
    }
    super.initializePool(globalMemStoreSize, 1.0F, 1.0F, heapMemoryManager);
    assert pool != null;
  }

  protected Chunk getChunk() {
    // In case of DurableChunks, it has to come always from pool. Never create on demand. Return
    // null when no free chunk available. The null chunks to be handled down the line.
    Chunk chunk = this.pool.getChunk();
    if (chunk != null) {
      // put this chunk into the chunkIdMap
      this.chunkIdMap.put(chunk.getId(), new SoftReference<>(chunk));
    } else {
      if (LOG.isTraceEnabled()) {
        LOG.trace("The chunk pool is full. .");
        // TODO
      }
    }
    return chunk;
  }

  @Override
  protected Chunk createChunk(boolean pool) {
    assert pool;
    int id = chunkID.getAndIncrement();
    assert id > 0;
    long offsetToUse = this.offset.getAndAdd(chunkSize);
    return new DurableSlicedChunk(id, this.durableBigChunk, offsetToUse, chunkSize);
  }

  protected void persist(Chunk c) {
    assert c instanceof DurableSlicedChunk;
    ((DurableSlicedChunk) c).persist();
  }

  // TODO seems no one calls me
  protected void close() {
    // The other problem here is that when there is an abrupt shutdown I think the chunk area
    // may be corrupted and may not be able to reuse it? Need to check.
    // TODO : Mnemonic currently deletes the path if it is already available - need to see
    // if it interferes with restart scenario. If needed need to handle those cases
    this.durableBigChunk.destroy();
  }
}
