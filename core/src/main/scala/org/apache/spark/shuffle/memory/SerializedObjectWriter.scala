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

package org.apache.spark.shuffle.memory

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.ByteBuffer

import org.apache.spark.util.io.ChunkedByteBuffer
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv
import org.apache.spark.serializer._
import org.apache.spark.storage.{BlockManager, ShuffleBlockId, StorageLevel}

/** Serializes and optionally compresses data into an in-memory byte stream. */
private[spark] class SerializedObjectWriter(blockManager: BlockManager,
                                            dep: ShuffleDependency[_, _, _],
                                            partitionId: Int,
                                            bucketId: Int) {

  /**
    * A ByteArrayOutputStream that will convert the underlying byte array to a byte buffer without
    * copying all of the data. This is to avoid calling the ByteArrayOutputStream.toByteArray
    * method, because that method makes a copy of the byte array.
    */
  private class ByteArrayOutputStreamWithZeroCopyByteBuffer extends ByteArrayOutputStream {
    def getByteBuffer(): ChunkedByteBuffer = {
      new ChunkedByteBuffer(Array(ByteBuffer.wrap(buf, 0, size())))
    }
  }

  private val byteOutputStream = new ByteArrayOutputStreamWithZeroCopyByteBuffer()
  private val ser = dep.serializer
  private val shuffleId = dep.shuffleId
  private val blockId = ShuffleBlockId(shuffleId, partitionId, bucketId)

  private val serializerManager = SparkEnv.get.serializerManager

  /* Only initialize compressionStream and serializationStream if some bytes are written, otherwise
   * 16 bytes will always be written to the byteOutputStream (and those bytes will be unnecessarily
   * transferred to reduce tasks). */
  private var initialized = false
  private var compressionStream: OutputStream = null
  private var serializationStream: SerializationStream = null

  def open() {
    compressionStream = serializerManager.wrapForCompression(blockId, byteOutputStream)
    serializationStream = ser.newInstance().serializeStream(compressionStream)
    initialized = true
  }

  def write(value: Any) {
    if (!initialized) {
      open()
    }
    serializationStream.writeObject(value)
  }

  // true if succeed, otherwise return false
  def close(saveToBlockManager: Boolean): Long = {
    if (initialized) {
      serializationStream.flush()
      serializationStream.close()
      if (saveToBlockManager) {
        val result = blockManager.putBytesAndReturnSize(
          blockId,
          byteOutputStream.getByteBuffer(),
          StorageLevel.MEMORY_ONLY_SER,
          tellMaster = false)
        return result
      }
    }
    return 0
  }
}