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

package org.apache.spark

import java.io.File

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.util.Utils

class MemoryShuffleSuite extends ShuffleSuite with BeforeAndAfterAll {

  // This test suite should run all tests in ShuffleSuite with sort-based shuffle.

  private var tempDir: File = _

  override def beforeAll() {
    super.beforeAll()
    conf.set("spark.shuffle.manager", "sort")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempDir = Utils.createTempDir()
    conf.set("spark.local.dir", tempDir.getAbsolutePath)
  }

  override def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterEach()
    }
  }

  test("memory shuffle simple test") {
    val myConf = conf.clone().set("spark.shuffle.compress", "false")
      .set("spark.shuffle.manager", "nocopy")
      //.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    sc = new SparkContext("local", "test", myConf)
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1)), 2)
    //val pairs = sc.parallelize(Array((1, 1), (2, 1)), 1)
    //val pairs = sc.parallelize(Array((1, 1)), 1)
    val groups = pairs.groupByKey(1).collect()
    assert(groups.size === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))
  }

  test("memory shuffle 2nd test") {
    val N = 1 << 3
    val myConf = conf.clone().set("spark.shuffle.compress", "false")
      .set("spark.shuffle.manager", "nocopy")
     .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc = new SparkContext("local", "test", myConf)
    var i = 0
    while (i < 1) {
      val pairs = sc.parallelize((1 to N).map(x => (x & 1, x)), 1)
      val start = System.nanoTime()
      val groups = pairs.groupByKey(1).count()
      val end = System.nanoTime()
      assert(groups == 2)
      i += 1
    }
  }

  test("memory shuffle benchmark") {
    val N = 1 << 20
    val myConf = conf.clone().set("spark.shuffle.compress", "false")
         .set("spark.shuffle.manager", "nocopy")
      //.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    sc = new SparkContext("local", "test", myConf)
    var i = 0
    val largeString =
      "gdfgsdfgsdgfdfsgsdgdsgsdfgsgdfgdsfgdafddfssdfsdf" +
      "gdfgsdfgsdgfdfsgsdgdsgsdfgsgdfgdsfgdafddfssdfsdf" +
      "gdfgsdfgsdgfdfsgsdgdsgsdfgsgdfgdsfgdafddfssdfsdf" +
      "gdfgsdfgsdgfdfsgsdgdsgsdfgsgdfgdsfgdafddfssdfsdf" +
      "gdfgsdfgsdgfdfsgsdgdsgsdfgsgdfgdsfgdafddfssdfsdf" +
      "gdfgsdfgsdgfdfsgsdgdsgsdfgsgdfgdsfgdafddfssdfsdf" +
      "gdfgsdfgsdgfdfsgsdgdsgsdfgsgdfgdsfgdafddfssdfsdf" +
      "gdfgsdfgsdgfdfsgsdgdsgsdfgsgdfgdsfgdafddfssdfsdf" +
      "gdfgsdfgsdgfdfsgsdgdsgsdfgsgdfgdsfgdafddfssdfsdf" +
      "gdfgsdfgsdgfdfsgsdgdsgsdfgsgdfgdsfgdafddfssdfsdf"
    while (i < 1) {
      val pairs = sc.parallelize((1 to N).map(x => (x & 7, largeString)), 1)
      val start = System.nanoTime()
      val groups = pairs.groupByKey(1).count()
      val end = System.nanoTime()
      println("time spent is " + (end - start) / 1000000 + " ms.")
      assert(groups == 8)
      i += 1
    }
  }

  test("memory shuffle benchmark, primitive types") {
    val N = 1 << 20
    val myConf = conf.clone().set("spark.shuffle.compress", "false")
      .set("spark.shuffle.manager", "sort")
    //.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    sc = new SparkContext("local", "test", myConf)
    var i = 0
    while (i < 1) {
      val pairs = sc.parallelize((1 to N).map(x => (x & 7, 1.0 * x)), 1)
      val start = System.nanoTime()
      val groups = pairs.groupByKey(1).count()
      val end = System.nanoTime()
      println("time spent is " + (end - start) / 1000000 + " ms.")
      assert(groups == 8)
      i += 1
    }
  }


  test("sort shuffle benchmark") {
    val N = 1 << 22
    val myConf = conf.clone().set("spark.shuffle.compress", "false")
      .set("spark.shuffle.manager", "sort")
    //.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    sc = new SparkContext("local", "test", myConf)
    var i = 0
    while (i < 1) {
      val pairs = sc.parallelize((1 to N).map(x => (x & 7, x)), 1)
      val start = System.nanoTime()
      val groups = pairs.groupByKey(1).count()
      val end = System.nanoTime()
      println("time spent is " + (end - start) / 1000000 + " ms.")
      //assert(groups == 8)
      i += 1
    }
  }

  // create a test that compares performance (dataframe)





}
