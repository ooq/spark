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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, DeclarativeAggregate}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._


class CodegenBytesToBytesMapGenerator(
                                  ctx: CodegenContext,
                                  aggregateExpressions: Seq[AggregateExpression],
                                  generatedClassName: String,
                                  groupingKeySchema: StructType,
                                  bufferSchema: StructType) {
  case class Buffer(dataType: DataType, name: String)
  val groupingKeys = groupingKeySchema.map(k => Buffer(k.dataType, ctx.freshName("key")))
  val bufferValues = bufferSchema.map(k => Buffer(k.dataType, ctx.freshName("value")))
  val groupingKeySignature =
    groupingKeys.map(key => s"${ctx.javaType(key.dataType)} ${key.name}").mkString(", ")
  val buffVars: Seq[ExprCode] = {
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
    val initExpr = functions.flatMap(f => f.initialValues)
    initExpr.map { e =>
      val isNull = ctx.freshName("bufIsNull")
      val value = ctx.freshName("bufValue")
      ctx.addMutableState("boolean", isNull, "")
      ctx.addMutableState(ctx.javaType(e.dataType), value, "")
      val ev = e.genCode(ctx)
      val initVars =
        s"""
           | $isNull = ${ev.isNull};
           | $value = ${ev.value};
       """.stripMargin
      ExprCode(ev.code + initVars, isNull, value)
    }
  }

  def generate(): String = {
    s"""
       |  import org.apache.spark.unsafe.Platform;
       |  import org.apache.spark.memory.TaskMemoryManager;
       |  import org.apache.spark.memory.MemoryConsumer;
       |  import org.apache.spark.unsafe.array.LongArray;
       |  import org.apache.spark.unsafe.memory.MemoryBlock;
       |
       |public class $generatedClassName extends MemoryConsumer{
       |${initializeAggregateHashMap()}
       |
       |${generateFindOrInsert()}
       |
       |${generateAcquireNewPage()}
       |
       |${generateEquals()}
       |
       |${generateArrayEquals()}
       |
       |${generateHashFunction()}
       |
       |${generateKVIterator()}
       |
       |${generateClose()}
       |}
     """.stripMargin
  }

  private def initializeAggregateHashMap(): String = {
    val generatedSchema: String =
      s"new org.apache.spark.sql.types.StructType()" +
        (groupingKeySchema ++ bufferSchema).map { key =>
          key.dataType match {
            case d: DecimalType =>
              s""".add("${key.name}", org.apache.spark.sql.types.DataTypes.createDecimalType(
                  |${d.precision}, ${d.scale}))""".stripMargin
            case _ =>
              s""".add("${key.name}", org.apache.spark.sql.types.DataTypes.${key.dataType})"""
          }
        }.mkString("\n").concat(";")

    val generatedGroupingKeySchema: String =
      s"new org.apache.spark.sql.types.StructType()" +
        groupingKeySchema.map { key =>
          key.dataType match {
            case d: DecimalType =>
              s""".add("${key.name}", org.apache.spark.sql.types.DataTypes.createDecimalType(
                  |${d.precision}, ${d.scale}))""".stripMargin
            case _ =>
              s""".add("${key.name}", org.apache.spark.sql.types.DataTypes.${key.dataType})"""
          }
        }.mkString("\n").concat(";")

    val generatedAggBufferSchema: String =
      s"new org.apache.spark.sql.types.StructType()" +
        bufferSchema.map { key =>
          key.dataType match {
            case d: DecimalType =>
              s""".add("${key.name}", org.apache.spark.sql.types.DataTypes.createDecimalType(
                  |${d.precision}, ${d.scale}))""".stripMargin
            case _ =>
              s""".add("${key.name}", org.apache.spark.sql.types.DataTypes.${key.dataType})"""
          }
        }.mkString("\n").concat(";")

    s"""
       |
       |
       |  private int capacity = 1 << 16;
       |  private double loadFactor = 0.5;
       |  private int maxSteps= 2;
       |  private int numRows = 0;
       |  private org.apache.spark.sql.types.StructType schema = $generatedSchema
       |  private org.apache.spark.sql.types.StructType aggregateBufferSchema =
       |    $generatedAggBufferSchema
       |  private org.apache.spark.sql.types.StructType groupingKeySchema =
       |    $generatedGroupingKeySchema
       |  LongArray longArray;
       |  private int mask;
       |  private TaskMemoryManager taskMemoryManager;
       |  private final LinkedList<MemoryBlock> dataPages = new LinkedList<>();
       |  private MemoryBlock currentPage = null;
       |  private long pageCursor = 0;
       |  private final byte[] emptyAggregationBuffer;
       |
       |  private Object vbase;
       |  private long voff;
       |  private long vlen;
       |
       |  // a re-used pointer to the current aggregation buffer
       |  private final UnsafeRow currentAggregationBuffer;
       |
       |  public $generatedClassName(
       |    TaskMemoryManager taskMemoryManager,
       |    InternalRow emptyAggregationBuffer) {
       |
       |    this.taskMemoryManager = taskMemoryManager;
       |    longArray = allocateArray(capacity * 2);
       |    longArray.zeroOut();
       |    mask = capacity - 1;
       |
       |    final UnsafeProjection valueProjection = UnsafeProjection.create(aggregationBufferSchema);
       |    this.emptyAggregationBuffer = valueProjection.apply(emptyAggregationBuffer).getBytes();
       |
       |    vbase = emptyAggregationBuffer.getBaseObject();
       |    voff = emptyAggregationBuffer.getBaseOffset();
       |    vlen = emptyAggregationBuffer.getSizeInBytes();
       |
       |  }
     """.stripMargin
  }

  /**
    * Generates a method that computes a hash by currently xor-ing all individual group-by keys. For
    * instance, if we have 2 long group-by keys, the generated function would be of the form:
    *
    * {{{
    * private long hash(long agg_key, long agg_key1) {
    *   return agg_key ^ agg_key1;
    *   }
    * }}}
    */
  private def generateHashFunction(): String = {
    val hash = ctx.freshName("hash")

    def genHashForKeys(groupingKeys: Seq[Buffer]): String = {
      groupingKeys.map { key =>
        val result = ctx.freshName("result")
        s"""
           |${genComputeHash(ctx, key.name, key.dataType, result)}
           |$hash = ($hash ^ (0x9e3779b9)) + $result + ($hash << 6) + ($hash >>> 2);
          """.stripMargin
      }.mkString("\n")
    }

    s"""
       |private long hash($groupingKeySignature) {
       |  long $hash = 0;
       |  ${genHashForKeys(groupingKeys)}
       |  return $hash;
       |}
     """.stripMargin
  }

  /**
    * Generates a method that returns true if the group-by keys exist at a given index in the
    * associated [[org.apache.spark.sql.execution.vectorized.ColumnarBatch]]. For instance, if we
    * have 2 long group-by keys, the generated function would be of the form:
    *
    * {{{
    * private boolean equals(int idx, long agg_key, long agg_key1) {
    *   return batch.column(0).getLong(buckets[idx]) == agg_key &&
    *     batch.column(1).getLong(buckets[idx]) == agg_key1;
    * }
    * }}}
    */
  private def generateEquals(): String = {

    def genEqualsForKeys(groupingKeys: Seq[Buffer]): String = {
      groupingKeys.zipWithIndex.map { case (key: Buffer, ordinal: Int) =>
        s"""(${ctx.genEqual(key.dataType, ctx.getValue("batch", "buckets[idx]",
          key.dataType, ordinal), key.name)})"""
      }.mkString(" && ")
    }

    s"""
       |private boolean equals(int idx, $groupingKeySignature) {
       |  return ${genEqualsForKeys(groupingKeys)};
       |}
     """.stripMargin
  }

  private def generateArrayEquals(): String = {
    s"""
       |private boolean arrayEquals(
       |  Object leftBase, long leftOffset, Object rightBase, long rightOffset, final long length) {
       |      int i = 0;
       |    while (i <= length - 8) {
       |      if (Platform.getLong(leftBase, leftOffset + i) !=
       |        Platform.getLong(rightBase, rightOffset + i)) {
       |        return false;
       |      }
       |      i += 8;
       |    }
       |    while (i < length) {
       |      if (Platform.getByte(leftBase, leftOffset + i) !=
       |        Platform.getByte(rightBase, rightOffset + i)) {
       |        return false;
       |      }
       |      i += 1;
       |    }
       |    return true;
       |
       |
       |}
     """.stripMargin
  }

  /**
    * Generate a method that acquires a new memory page
    *
    * @return
    */
  private def generateAcquireNewPage(): String = {

    s"""
       |private boolean acquireNewPage(long required) {
       |    try {
       |      currentPage = allocatePage(required);
       |    } catch (OutOfMemoryError e) {
       |      return false;
       |    }
       |    Platform.putInt(currentPage.getBaseObject(), currentPage.getBaseOffset(), 0);
       |    pageCursor = 4;
       |
       |    dataPages.add(currentPage);
       |     //TODO: add code to recycle the pages when we destroy this map
       |    return true;
       |}
     """.stripMargin
  }

  /**
    * Generates a method that returns a mutable
    * [[org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row]] which keeps track of the
    * aggregate value(s) for a given set of keys. If the corresponding row doesn't exist, the
    * generated method adds the corresponding row in the associated
    * [[org.apache.spark.sql.execution.vectorized.ColumnarBatch]]. For instance, if we
    * have 2 long group-by keys, the generated function would be of the form:
    *
    * {{{
    * public org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row findOrInsert(
    *     long agg_key, long agg_key1) {
    *   long h = hash(agg_key, agg_key1);
    *   int step = 0;
    *   int idx = (int) h & (numBuckets - 1);
    *   while (step < maxSteps) {
    *     // Return bucket index if it's either an empty slot or already contains the key
    *     if (buckets[idx] == -1) {
    *       batch.column(0).putLong(numRows, agg_key);
    *       batch.column(1).putLong(numRows, agg_key1);
    *       batch.column(2).putLong(numRows, 0);
    *       buckets[idx] = numRows++;
    *       return batch.getRow(buckets[idx]);
    *     } else if (equals(idx, agg_key, agg_key1)) {
    *       return batch.getRow(buckets[idx]);
    *     }
    *     idx = (idx + 1) & (numBuckets - 1);
    *     step++;
    *   }
    *   // Didn't find it
    *   return null;
    * }
    * }}}
    */
  private def generateFindOrInsert(): String = {

    def genCodeToSetKeys(groupingKeys: Seq[Buffer]): Seq[String] = {
      groupingKeys.zipWithIndex.map { case (key: Buffer, ordinal: Int) =>
        ctx.setValue("batch", "numRows", key.dataType, ordinal, key.name)
      }
    }

    def genCodeToSetAggBuffers(bufferValues: Seq[Buffer]): Seq[String] = {
      bufferValues.zipWithIndex.map { case (key: Buffer, ordinal: Int) =>
        ctx.updateColumn("batch", "numRows", key.dataType, groupingKeys.length + ordinal,
          buffVars(ordinal), nullable = true)
      }
    }

    s"""
       |public UnsafeRow findOrInsert(UnsafeRow rowKey, ${
      groupingKeySignature}) {
       |  long h = hash(${groupingKeys.map(_.name).mkString(", ")});
       |  int step = 1;
       |  int pos = (int) h & mask;
       |  Object kbase = rowKey.getBaseObject(); // could be cogen
       |  long koff = rowKey.getBaseOffset();  // could be cogen
       |  long klen = rowKey.getSizeInBytes(); // could be cogen
       |  while (step < maxSteps) {
       |    if (longArray.get(pos * 2) == 0) { //new entry
       |      if (numRows < capacity) {
       |
       |        // do append
       |        final long recordLength = 8 + klen + vlen + 8;
       |        if (currentPage == null || currentPage.size() - pageCursor < recordLength) {
       |          // acquire new page
       |          if( !acquireNewPage(recordLength + 4L)) {
       |            return null;
       |          }
       |        }
       |
       |        // Initialize aggregate keys
       |        // append the keys and values to current data page
       |        final Object base = currentPage.getBaseObject();
       |        long offset = currentPage.getBaseeOffset() + pageCursor;
       |        final long recordOffset = offset;
       |        Platform.putInt(base, offset, klen + vlen + 4);
       |        Platform.putInt(base, offset + 4, klen);
       |        offset += 8;
       |        Platform.copyMemory(kbase, koff, base, offset, klen);
       |        offset += klen;
       |        Platform.copyMemory(vbase, voff, base, offset, vlen);
       |        offset += vlen;
       |        // put this value at the beginning of the list
       |        Platform.putLong(base, offset, 0);
       |
       |        // Update bookkeeping data structures -----------------------------
       |        offset = currentPage.getBaseOffset();
       |        Platform.putInt(base, offset, Platform.getInt(base, offset) + 1);
       |        pageCursor += recordLength;
       |        final long storedKeyAddress = taskMemoryManager.encodePageNumberAndOffset(
       |        currentPage, recordOffset);
       |        longArray.set(pos * 2, storedKeyAddress);
       |        longArray.set(pos * 2 + 1, keyHashcode);
       |
       |        // now we want point the value UnsafeRow to the correct location
       |        // basically valuebase, valueoffset and value length
       |        currentAggregationBuffer.pointTo(base, recordOffset + 8 + klen, vlen);
       |        return currentAggregationBuffer;
       |      } else {
       |        // No more space
       |        return null;
       |      }
       |    } else {
       |      // we will check equality here and return buffer if matched
       |      // 1st level: hash code match
       |      long stored = longArray.get(pos * 2 + 1);
       |      if ((int) (stored) == hash) {
       |          // 2nd level: keys match
       |          // TODO: codegen based with key types, so we don't need byte by byte compare
       |          long foundFullKeyAddress = longArray.get(pos * 2);
       |          Object foundBase = taskMemoryManager.getPage(foundFullKeyAddress);
       |          long foundOff = taskMemoryManager.getOffsetInPage(foundFullKeyAddress)) + 8;
       |          long foundLen = Platform.getInt(foundBase, foundOff-4);
       |          if ((int) foundLen == klen &&
       |              arrayEquals(kbase, koff, foundBase, foundOff, klen)) {
       |            currentAggregationBuffer.pointTo(base, recordOffset + 8 + klen, vlen);
       |          }
       |      }
       |
       |    }
       |    // move on to the next position
       |    // TODO: change the strategies
       |    // now triangle probing
       |    pos = (pos + step) & mask;
       |    step++;
       |  }
       |  // Didn't find it
       |  return null;
       |}
     """.stripMargin
  }

  private def generateKVIterator(): String = {
    s"""
       |
       |  public KVIterator<UnsafeRow, UnsafeRow> iterator() {
       |    return new KVIterator<UnsafeRow, UnsafeRow>() {
       |
       |      private final UnsafeRow key = new UnsafeRow(groupingKeySchema.length());
       |      private final UnsafeRow value = new UnsafeRow(aggregationBufferSchema.length());
       |
       |      private MemoryBlock currentPage = null;
       |      private Object pageBaseObject = null;
       |      private long offsetInPage = 0;
       |      private int recordsInPage = 0;
       |
       |      private int klen;
       |      private int vlen;
       |      private int totalLength;
       |
       |
       |      if (dataPages.size() > 0) {
       |        currentPage = dataPages.remove();
       |        pageBaseObject = currentPage.getBaseObject();
       |        offsetInPage = currentPage.getBaseOffset();
       |        recordsInPage = Platform.getInt(pageBaseObject, offsetInPage);
       |        offsetInPage += 4;
       |      }
       |
       |      @Override
       |      public boolean next() {
       |        //searching for the next non empty page is records is now zero
       |        while (recordsInPage == 0) {
       |          if (!advanceToNextPage()) return false;
       |        }
       |
       |        totalLength = Platform.getInt(pageBaseObject, offsetInPage);
       |        klen = Platform.getInt(pageBaseObject, offsetInPage + 4);
       |        vlen = totalLength - klen;
       |
       |        key.pointTo(pageBaseObject, offsetInPage + 8, klen);
       |        value.pointtTo(pageBaseObject, offsetInPage + 8 + klen, vlen);
       |        offsetInpage += 4 + totalLength + 8;
       |        recordsInPage -= 1;
       |        return true;
       |      }
       |
       |      @Override
       |      public UnsafeRow getKey() {
       |        return key;
       |      }
       |
       |      @Override
       |      public UnsafeRow getValue() {
       |        return value;
       |      }
       |
       |      @Override
       |      public void close() {
       |        // Do nothing.
       |      }
       |
       |      private boolean advanceToNextPage() {
       |        if (dataPages.size() > 0) {
       |          currentPage = dataPages.remove();
       |          pageBaseObject = currentPage.getBaseObject();
       |          offsetInPage = currentPage.getbaseOffset();
       |          recordsInPage = Platform.getInt(pageBaseObject, offsetInPage);
       |          offsetInpPage += 4;
       |        } else {
       |          return false;
       |        }
       |      }
       |    };
       |  }
       |
       |
       |
       |
     """.stripMargin
  }

  private def generateClose(): String = {
    s"""
       |public void close() {
       |  batch.close();
       |}
     """.stripMargin
  }

  private def genComputeHash(
                              ctx: CodegenContext,
                              input: String,
                              dataType: DataType,
                              result: String): String = {
    def hashInt(i: String): String = s"int $result = $i;"
    def hashLong(l: String): String = s"long $result = $l;"
    def hashBytes(b: String): String = {
      val hash = ctx.freshName("hash")
      s"""
         |int $result = 0;
         |for (int i = 0; i < $b.length; i++) {
         |  ${genComputeHash(ctx, s"$b[i]", ByteType, hash)}
         |  $result = ($result ^ (0x9e3779b9)) + $hash + ($result << 6) + ($result >>> 2);
         |}
       """.stripMargin
    }

    dataType match {
      case BooleanType => hashInt(s"$input ? 1 : 0")
      case ByteType | ShortType | IntegerType | DateType => hashInt(input)
      case LongType | TimestampType => hashLong(input)
      case FloatType => hashInt(s"Float.floatToIntBits($input)")
      case DoubleType => hashLong(s"Double.doubleToLongBits($input)")
      case d: DecimalType =>
        if (d.precision <= Decimal.MAX_LONG_DIGITS) {
          hashLong(s"$input.toUnscaledLong()")
        } else {
          val bytes = ctx.freshName("bytes")
          s"""
            final byte[] $bytes = $input.toJavaBigDecimal().unscaledValue().toByteArray();
            ${hashBytes(bytes)}
          """
        }
      case StringType => hashBytes(s"$input.getBytes()")
    }
  }
}
