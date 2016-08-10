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

package org.apache.spark.shuffle.page

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.ByteBuffer

import scala.collection.mutable.LinkedList
import scala.collection.mutable.Queue
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.util.io.ChunkedByteBuffer
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv
import org.apache.spark.serializer._
import org.apache.spark.storage.{BlockManager, ShuffleBlockId, StorageLevel}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.MemoryBlock

/** Serializes and optionally compresses data into an in-memory byte stream. */
private[spark] class PageCopyObjectWriter(
    manager: BlockManager,
    dep: ShuffleDependency[_, _, _],
    partitionId: Int,
    bucketId: Int,
    memoryManager: TaskMemoryManager)
  extends MemoryConsumer (memoryManager, 64 * 1024 * 1024, MemoryMode.ON_HEAP) {


  private val blockManager = manager
  private val ser = dep.serializer
  private val shuffleId = dep.shuffleId
  private val blockId = ShuffleBlockId(shuffleId, partitionId, bucketId)
  private var initialized = false
  private val dataPages = Queue[MemoryBlock]()
  private var currentPage: MemoryBlock = null
  private var base: Object = null
  private var baseOff: Long = -1
  private var pageCursor: Long = -1

  private def acquireNewPage (required: Long) : Boolean = {
    try {
      currentPage = allocatePage(required)
      base = currentPage.getBaseObject
      baseOff = currentPage.getBaseOffset
    }
    catch {
      case (e: OutOfMemoryError) => return false
    }
    dataPages.enqueue(currentPage)
    Platform.putInt(currentPage.getBaseObject, currentPage.getBaseOffset, 0)
    pageCursor = 4
    return true
  }

  def open() {
    println("opening UnserializedObjectWriter writer " + ser)
    initialized = true
  }

  def write(key: Any, value: Any) {
    if (!initialized) {
      open()
    }
    println("in PageCopyObjectWriter " + key + " " + value)
    //new Throwable().printStackTrace()
    //buffer.enqueue(key)
    //do nothing to key, assuming zero
    writeValueToPage(value)
  }


  private def writeValueToPage(value: Any): Unit = {
    // format:
    // 4 byte for number of records
    // 4 byte for size = l
    // UnsafeRow of length l
    /*
    val recordLength: Integer = value.getSizeInBytes + 4
    if (currentPage == null || currentPage.size - pageCursor < recordLength) {
      if (!acquireNewPage(recordLength + 4L)) {
        return
      }
    }
    Platform.putInt(base, baseOff + pageCursor, recordLength)
    Platform.copyMemory(value.getBaseObject, value.getBaseOffset,
      base, baseOff, value.getSizeInBytes)
    pageCursor += recordLength
    Platform.putInt(base, baseOff, Platform.getInt(base, baseOff) + 1)
    */
  }


  // true if succeed, otherwise return false
  def close(saveToBlockManager: Boolean): Long = {
    if (initialized) {
      assert(saveToBlockManager)
      if (saveToBlockManager) {
        blockManager.putMyPage(blockId, dataPages)
        return 1
      }
    }
    return 0
  }


  def spill(size: Long, trigger: MemoryConsumer): Long = {
    println("Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.")
    return 0
  }
}