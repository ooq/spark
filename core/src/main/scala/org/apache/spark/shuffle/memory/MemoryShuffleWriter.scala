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

import org.apache.spark.{ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.{SerializationStream, Serializer}
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.storage.{BlockManager, ShuffleBlockId, StorageLevel}

/** A ShuffleWriter that stores all shuffle data in memory using the block manager. */
private[spark] class MemoryShuffleWriter[K, V](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, _],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] {

  val dep = handle.dependency

  // Create a different writer for each output bucket.
  val blockManager = SparkEnv.get.blockManager
  val numBuckets = dep.partitioner.numPartitions
  val shuffleData = Array.tabulate[SerializedObjectWriter](numBuckets) {
    bucketId =>
      new SerializedObjectWriter(blockManager, dep, mapId, bucketId)
  }

  val shuffleWriteMetrics = context.taskMetrics().shuffleWriteMetrics

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val iter = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        dep.aggregator.get.combineValuesByKey(records, context)
      } else {
        records
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      records
    }


    // Write the data to the appropriate bucket.
    for (elem <- iter) {
      val bucketId = dep.partitioner.getPartition(elem._1)
      shuffleData(bucketId).write(elem)
      shuffleWriteMetrics.incRecordsWritten(1)
    }
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    // Store the shuffle data in the block manager (if the shuffle was successful) and update the
    // bytes written in ShuffleWriteMetrics.
    val sizes = shuffleData.map { shuffleWriter =>
      val bytesWritten = shuffleWriter.close(success)
      shuffleWriteMetrics.incBytesWritten(bytesWritten)
      bytesWritten
    }
    if (success) {
      shuffleBlockResolver.removeDataByMap(dep.shuffleId, mapId)
      Some(MapStatus(SparkEnv.get.blockManager.blockManagerId, sizes))
    } else {
      None
    }
  }
}

