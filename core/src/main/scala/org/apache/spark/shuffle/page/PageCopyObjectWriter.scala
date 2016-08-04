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

import scala.collection.mutable.{Queue}

import org.apache.spark.util.io.ChunkedByteBuffer
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv
import org.apache.spark.serializer._
import org.apache.spark.storage.{BlockManager, ShuffleBlockId, StorageLevel}

/** Serializes and optionally compresses data into an in-memory byte stream. */
private[spark] class PageCopyObjectWriter(manager: BlockManager,
                                              dep: ShuffleDependency[_, _, _],
                                              partitionId: Int,
                                              bucketId: Int) {

  private val blockManager = manager
  private val ser = dep.serializer
  private val shuffleId = dep.shuffleId
  private val blockId = ShuffleBlockId(shuffleId, partitionId, bucketId)
  private var initialized = false
  private val buffer = new Queue[Any]()

  def open() {
    println("opening UnserializedObjectWriter writer " + ser)
    initialized = true
  }

  def write(value: Any) {
    if (!initialized) {
      open()
    }
    buffer.enqueue(value)
  }

  def write(key: Any, value: Any) {
    if (!initialized) {
      open()
    }
    println("in PageCopyObjectWriter " + key + " " + value)
    new Throwable().printStackTrace()
    buffer.enqueue(key)
    buffer.enqueue(value)
  }

  // true if succeed, otherwise return false
  def close(saveToBlockManager: Boolean): Long = {
    if (initialized) {
      assert(saveToBlockManager)
      if (saveToBlockManager) {
        blockManager.putMyBuffer(blockId, buffer)
        return 1
      }
    }
    return 0
  }
}