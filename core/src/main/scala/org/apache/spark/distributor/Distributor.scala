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

package org.apache.spark.distributor

import java.io._
import java.nio.ByteBuffer
import javax.annotation.concurrent.NotThreadSafe

import scala.collection.mutable.Queue
import scala.reflect.ClassTag
import org.apache.spark.SparkEnv
import org.apache.spark.annotation.{DeveloperApi, Private}
import org.apache.spark.util.NextIterator
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.memory.{MemoryConsumer, TaskMemoryManager}

@DeveloperApi
abstract class Distributor {

  @volatile protected var defaultClassLoader: Option[ClassLoader] = None

  def setDefaultClassLoader(classLoader: ClassLoader): Distributor = {
    defaultClassLoader = Some(classLoader)
    this
  }
  def newInstance(): DistributorInstance
}

@DeveloperApi
@NotThreadSafe
abstract class DistributorInstance {
  def distributeStream(taskMemoryManager: TaskMemoryManager): DistributeStream
  def fetchStream(taskMemoryManager: TaskMemoryManager, pages: Queue[MemoryBlock]): FetchStream
}

@DeveloperApi
abstract class DistributeStream (taskMemoryManager: TaskMemoryManager)
  extends MemoryConsumer (taskMemoryManager) {
  /** The most general-purpose method to write an object. */
  def writeObject[T: ClassTag](t: T): DistributeStream
  /** Writes the object representing the key of a key-value pair. */
  def writeKey[T: ClassTag](key: T): DistributeStream = writeObject(key)
  /** Writes the object representing the value of a key-value pair. */
  def writeValue[T: ClassTag](value: T): Boolean
  def flush(): Unit
  def close(): Unit
  def getMemoryPages(): Queue[MemoryBlock]

  def writeAll[T: ClassTag](iter: Iterator[T]): DistributeStream = {
    while (iter.hasNext) {
      writeObject(iter.next())
    }
    this
  }
}

@DeveloperApi
abstract class FetchStream (taskMemoryManager: TaskMemoryManager)
extends MemoryConsumer (taskMemoryManager) {
  /** The most general-purpose method to read an object. */
  def readObject[T: ClassTag](): T
  /** Reads the object representing the key of a key-value pair. */
  def readKey[T: ClassTag](): T = readObject[T]()
  /** Reads the object representing the value of a key-value pair. */
  def readValue[T: ClassTag](): T = readObject[T]()
  def close(): Unit

  /**
    * Read the elements of this stream through an iterator. This can only be called once, as
    * reading each element will consume data from the input source.
    */
  def asIterator: Iterator[Any] = new NextIterator[Any] {
    override protected def getNext() = {
      try {
        readObject[Any]()
      } catch {
        case eof: EOFException =>
          finished = true
          null
      }
    }

    override protected def close() {
      FetchStream.this.close()
    }
  }

  /**
    * Read the elements of this stream through an iterator over key-value pairs. This can only be
    * called once, as reading each element will consume data from the input source.
    */
  def asKeyValueIterator: Iterator[(Any, Any)]
}
