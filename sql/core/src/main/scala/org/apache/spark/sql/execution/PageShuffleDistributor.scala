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

import com.google.common.io.ByteStreams

import org.apache.spark.distributor.{DistributeStream, FetchStream, Distributor, DistributorInstance}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.unsafe.Platform


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
  override def distributeStream: DistributeStream = new DistributeStream {
    private[this] var writeBuffer: Array[Byte] = new Array[Byte](4096)

    override def writeValue[T: ClassTag](value: T): DistributeStream = {
      this
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
    }

    override def close(): Unit = {
      writeBuffer = null
    }
  }

  override def fetchStream: FetchStream = {
    new FetchStream {
      private[this] var row: UnsafeRow = new UnsafeRow(numFields)

      override def asKeyValueIterator: Iterator[(Int, UnsafeRow)] = {
        return null
      }

      override def asIterator: Iterator[Any] = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def readKey[T: ClassTag](): T = {
        // We skipped serialization of the key in writeKey(), so just return a dummy value since
        // this is going to be discarded anyways.
        null.asInstanceOf[T]
      }

      override def readValue[T: ClassTag](): T = {
        row.asInstanceOf[T]
      }

      override def readObject[T: ClassTag](): T = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def close(): Unit = {
      }
    }
  }
}
