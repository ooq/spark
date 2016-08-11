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

package org.apache.spark.shuffle.page;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import scala.Option;
import scala.Product2;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.*;
import org.apache.spark.annotation.Private;
import org.apache.spark.distributor.DistributeStream;
import org.apache.spark.distributor.FetchStream;
import org.apache.spark.distributor.Distributor;
import org.apache.spark.distributor.DistributorInstance;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.io.CompressionCodec$;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.PageShuffleBlockResolver;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.TimeTrackingOutputStream;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.Utils;
import org.apache.spark.storage.ShuffleBlockId;


@Private
public class PageShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  private final Logger logger = LoggerFactory.getLogger(PageShuffleWriter.class);

  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  private final BlockManager blockManager;
  private final PageShuffleBlockResolver shuffleBlockResolver;
  private final TaskMemoryManager memoryManager;
  private final SerializerInstance serializer;
  private final DistributorInstance distributor;
  private final Partitioner partitioner;
  private final ShuffleWriteMetrics writeMetrics;
  private final int shuffleId;
  private final int mapId;
  private final TaskContext taskContext;
  private final SparkConf sparkConf;
  private final boolean transferToEnabled;

  private DistributeStream distributeStream;
  @Nullable private MapStatus mapStatus;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private boolean stopping = false;

  public PageShuffleWriter(
          BlockManager blockManager,
          PageShuffleBlockResolver shuffleBlockResolver,
          TaskMemoryManager memoryManager,
          PageShuffleHandle<K, V> handle,
          int mapId,
          TaskContext taskContext,
          SparkConf sparkConf) throws IOException {
    final int numPartitions = handle.dependency().partitioner().numPartitions();
    this.blockManager = blockManager;
    this.shuffleBlockResolver = shuffleBlockResolver;
    this.memoryManager = memoryManager;
    this.mapId = mapId;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.shuffleId = dep.shuffleId();
    this.serializer = dep.serializer().newInstance();
    System.err.println("Our distributor is " + dep.distributor() + " PageShuffleWriter");
    assert(dep.distributor() != null);
    this.distributor = dep.distributor().newInstance();
    this.partitioner = dep.partitioner();
    this.writeMetrics = taskContext.taskMetrics().shuffleWriteMetrics();
    this.taskContext = taskContext;
    this.sparkConf = sparkConf;
    this.transferToEnabled = sparkConf.getBoolean("spark.file.transferTo", true);
    open();
  }

  @Override
  public void write(scala.collection.Iterator<Product2<K, V>> records) throws IOException {
    System.out.println("Write in PageShuffleWriter");
    while (records.hasNext()) {
      insertRecordIntoDistributor(records.next());
    }
    closeAndWriteOutput();
  }

  private void open() throws IOException {
    distributeStream = distributor.distributeStream();
  }

  @VisibleForTesting
  void closeAndWriteOutput() throws IOException {
    final long[] partitionLengths = new long[partitioner.numPartitions()];
    int i = 0;
    while (i < partitioner.numPartitions()) {
      partitionLengths[i] = 1;
      i++;
    }

    ShuffleBlockId blockId = new ShuffleBlockId(shuffleId, mapId, 0);
    blockManager.putMyPage(blockId, distributeStream.getMemoryPages());

    mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
  }

  @VisibleForTesting
  void insertRecordIntoDistributor(Product2<K, V> record) throws IOException {
    final K key = record._1();
    final int partitionId = partitioner.getPartition(key);

    distributeStream.writeKey(key, OBJECT_CLASS_TAG);
    distributeStream.writeValue(record._2(), OBJECT_CLASS_TAG);
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      if (stopping) {
        return Option.apply(null);
      } else {
        stopping = true;
        if (success) {
          if (mapStatus == null) {
            throw new IllegalStateException("Cannot call stop(true) without having called write()");
          }
          return Option.apply(mapStatus);
        } else {
          // The map task failed, so delete our output data.
          // shuffleBlockResolver.removeDataByMap(shuffleId, mapId);
          return Option.apply(null);
        }
      }
    } finally {
        // If sorter is non-null, then this implies that we called stop() in response to an error,
        // so we need to clean up memory and spill files created by the sorter
        //sorter.cleanupResources();
    }
  }

}
