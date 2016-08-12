/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage

import scala.collection.mutable.Queue

import org.apache.spark.internal.Logging
import org.apache.spark.util.NextIterator
import org.apache.spark.TaskContext
import org.apache.spark.unsafe.memory.MemoryBlock


private[spark]
final class PageShuffleBlockIterator(
                  context: TaskContext,
                  blockManager: BlockManager,
                  blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])])
  extends Iterator[Queue[MemoryBlock]] with Logging {

  private[this] var numBlocksToFetch = 0

  private[this] var numBlocksProcessed = 0

  private[this] val localBlocks = new Queue[BlockId]()

  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch

  initialize()

  private[this] def initialize() {
    var totalBlocks = 0
    for ((address, blockInfos) <- blocksByAddress) {
      totalBlocks += blockInfos.size
      logInfo("address: " + address + " id: "
        + address.executorId + " thisId: " + blockManager.blockManagerId.executorId)
      assert(address.executorId == blockManager.blockManagerId.executorId)
      localBlocks ++= blockInfos.filter(_._2 != 0).map(_._1)
      numBlocksToFetch += localBlocks.size
    }
    logInfo(s"Getting $numBlocksToFetch non-empty blocks out of $totalBlocks blocks")
  }

  override def next(): (Queue[MemoryBlock]) = {
    numBlocksProcessed += 1
    val blockId = localBlocks.dequeue()
    //println("Reading block " + blockId + " num of pages " + blockManager.getMyPage(blockId).length)
    blockManager.getMyPage(blockId)
  }
}
