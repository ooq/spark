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

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.shuffle._

/**
 * A ShuffleManager that stores shuffle data in-memory.
 * This shuffle manager uses hashing: it creates one in-memory block per reduce partition on each
 * mapper.
 */
private[spark] class MemoryShuffleManager(conf: SparkConf) extends ShuffleManager {
  private[this] val numMapsForShuffle = new ConcurrentHashMap[Int, Int]()

  override val shuffleBlockResolver = new IndexShuffleBlockResolver(conf)

  override def registerShuffle[K, V, C](
                                         shuffleId: Int,
                                         numMaps: Int,
                                         dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    new BaseShuffleHandle(shuffleId, numMaps, dependency)
  }

  override def getReader[K, C](
                                handle: ShuffleHandle,
                                startPartition: Int,
                                endPartition: Int,
                                context: TaskContext): ShuffleReader[K, C] = {
    new BlockStoreShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int,
                               context: TaskContext): ShuffleWriter[K, V] = {
    new MemoryShuffleWriter(
      shuffleBlockResolver, handle.asInstanceOf[BaseShuffleHandle[K, V, _]], mapId, context)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(numMapsForShuffle.remove(shuffleId)).foreach { numMaps =>
      (0 until numMaps).foreach { mapId =>
        shuffleBlockResolver.removeDataByMap(shuffleId, mapId)
      }
    }
    true
  }

  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }
}