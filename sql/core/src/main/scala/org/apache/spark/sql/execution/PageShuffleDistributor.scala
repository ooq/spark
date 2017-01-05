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

package org.apache.spark.sql.execution

import java.io._
import java.nio.ByteBuffer

import scala.reflect.ClassTag
import scala.collection.mutable.Queue
import com.google.common.io.ByteStreams
import org.apache.spark.distributor.{DistributeStream, Distributor, DistributorInstance, FetchStream}
import org.apache.spark.memory.{MemoryConsumer, TaskMemoryManager}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.MemoryBlock


private[spark] class PageShuffleDistributeStream(taskMemoryManager: TaskMemoryManager)
  extends DistributeStream (taskMemoryManager) {
  private val dataPages = new Queue[MemoryBlock]
  private var currentPage: MemoryBlock = null
  private var base: Object = null
  private var baseOff: Long = -1
  private var pageCursor: Long = -1

  def spill(size: Long, trigger: MemoryConsumer): Long = {
    println("Calling spill() on PageShuffleDistributeStream. Will not spill but return 0.")
    new Throwable().printStackTrace()
    return 0
  }

  private def acquireNewPage (required: Long) : Boolean = {
    try {
      //currentPage = allocatePage(required)

      currentPage = MemoryBlock.fromLongArray(new Array[Long](1 * 1000 * 1000))
      // currentPage = new MemoryBlock(null, Platform.allocateMemory(8 * 1000 * 1000), 8 * 1000 *
      //  1000)
      base = currentPage.getBaseObject
      baseOff = currentPage.getBaseOffset
    }
    catch {
      case (e: OutOfMemoryError) => return false
    }
    dataPages.enqueue(currentPage)
    Platform.putInt(base, baseOff, 0)
    pageCursor = 4
    return true
  }


  override def writeValue[T: ClassTag](value: T): Boolean = {
    // println("writing value in PageShuffleDistributor " + value)
    val row = value.asInstanceOf[UnsafeRow]
    val recordLength: Integer = row.getSizeInBytes + 4
    if (currentPage == null || currentPage.size - pageCursor < recordLength) {
      if (!acquireNewPage(recordLength + 4L)) {
        return false
      }
    }
    //println("record length is " + recordLength)

    Platform.putInt(base, baseOff + pageCursor, recordLength-4)
    Platform.copyMemory(row.getBaseObject, row.getBaseOffset,
      base, baseOff + pageCursor + 4, row.getSizeInBytes)
    pageCursor += recordLength
    Platform.putInt(base, baseOff, Platform.getInt(base, baseOff) + 1)
    true
  }

  override def writeKey[T: ClassTag](key: T): DistributeStream = {
    // The key is only needed on the map side when computing partition ids. It does not need to
    // be shuffled.
    assert(null == key || key.isInstanceOf[Int])
    this
  }

  override def writeAll[T: ClassTag](iter: Iterator[T]): DistributeStream = {
    // This method is never called by shuffle code.
    throw new UnsupportedOperationException
  }

  override def writeObject[T: ClassTag](t: T): DistributeStream = {
    // This method is never called by shuffle code.
    throw new UnsupportedOperationException
  }

  override def flush(): Unit = {
    throw new UnsupportedOperationException
  }

  override def close(): Unit = {
  }

  override def getMemoryPages: Queue[MemoryBlock] = {
    dataPages
  }
}

private[sql] class PageShuffleDistributor(
                          numFields: Int,
                          dataSize: SQLMetric = null) extends Distributor with Serializable {
  override def newInstance(): DistributorInstance =
    new PageShuffleDistributorInstance(numFields, dataSize)
}

private class PageShuffleDistributorInstance(
                                   numFields: Int,
                                   dataSize: SQLMetric) extends DistributorInstance {
  /**
    * Serializes a stream of UnsafeRows. Within the stream, each record consists of a record
    * length (stored as a 4-byte integer, written high byte first), followed by the record's bytes.
    */
  override def distributeStream(taskMemoryManager: TaskMemoryManager): DistributeStream
    = new PageShuffleDistributeStream(taskMemoryManager)

  override def fetchStream (taskMemoryManager: TaskMemoryManager,
                            pages: Queue[MemoryBlock]): FetchStream = {
    new FetchStream (taskMemoryManager) {
      private[this] val row: UnsafeRow = new UnsafeRow(numFields)

      override def asKeyValueIterator: Iterator[(Int, UnsafeRow)] = {
        new Iterator[(Int, UnsafeRow)] {
            private var currentPage: MemoryBlock = null
            private var currentNum = 0
            private var currentCursor = 0
            private var currentBase: Object = null
            private var currentOff: Long = 0
            private var finished = false
            private[this] var rowTuple: (Int, UnsafeRow) = (0, row)


          private def getNextRow() = {
              if (currentPage == null || currentNum == 0) {
                currentPage = pages.dequeue()
                currentBase = currentPage.getBaseObject
                currentOff = currentPage.getBaseOffset
                currentNum = Platform.getInt(currentBase, currentOff)
                currentCursor = 4
              }

              val l = Platform.getInt(currentBase, currentOff + currentCursor)
              row.pointTo(currentBase, currentOff + currentCursor + 4, l)

              currentNum -= 1
              currentCursor += l + 4

              if (currentNum == 0) currentPage = null
              //if (currentNum == 0) Platform.freeMemory(currentPage.getBaseOffset)
            }

          override def hasNext: Boolean = {
            (pages.length != 0 || currentNum != 0)
          }

          override def next() = {
                getNextRow()
                rowTuple
            }
          }
      }

      override def asIterator: Iterator[Any] = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def readKey[T: ClassTag](): T = {
        // We skipped serialization of the key in writeKey(), so just return a dummy value since
        // this is going to be discarded anyways.
        throw new UnsupportedOperationException
      }

      override def readValue[T: ClassTag](): T = {
        throw new UnsupportedOperationException
      }

      override def readObject[T: ClassTag](): T = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def close(): Unit = {
      }

      def spill(size: Long, trigger: MemoryConsumer): Long = {
        println("Calling spill() on fetchStream. Will not spill but return 0.")
        new Throwable().printStackTrace()
        return 0
      }

    }
  }
}
