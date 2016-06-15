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

/**
  * This is a helper class to generate an BytesToBytes hash map
  */
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
       |${generateEquals()}
       |
       |${generateHashFunction()}
       |
       |${generateRowIterator()}
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
       |  private static final int PAGE_NUMBER_BITS = 13;
       |  private static final int OFFSET_BITS = 64 - PAGE_NUMBER_BITS;  // 51
       |  private static final int PAGE_TABLE_SIZE = 1 << PAGE_NUMBER_BITS;
       |  public static final long MAXIMUM_PAGE_SIZE_BYTES = ((1L << 31) - 1) * 8L;
       |  private static final long MASK_LONG_LOWER_51_BITS = 0x7FFFFFFFFFFFFL;
       |
       |  private int capacity = 1 << 16;
       |  private double loadFactor = 0.5;
       |  private int maxSteps = 2;
       |  private int numRows = 0;
       |  private org.apache.spark.sql.types.StructType schema = $generatedSchema
       |  private org.apache.spark.sql.types.StructType aggregateBufferSchema =
       |    $generatedAggBufferSchema
       |  LongArray longArray;
       |  private int mask;
       |  private TaskMemoryManager taskMemoryManager;
       |
       |
       |  public $generatedClassName(TaskMemoryManager taskMemoryManager) {
       |
       |    this.taskMemoryManager = taskMemoryManager;
       |    longArray = allocateArray(capacity * 2);
       |    longArray.zeroOut();
       |    mask = capacity - 1;
       |
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
       |public UnsafeRow findOrInsert(${
      groupingKeySignature}) {
       |  long h = hash(${groupingKeys.map(_.name).mkString(", ")});
       |  int step = 0;
       |  int pos = (int) h & mask;
       |  while (step < maxSteps) {
       |    if (longArray.get(pos * 2) == 0) { //new entry
       |      if (numRows < capacity) {
       |
       |        // Initialize aggregate keys
       |        ${genCodeToSetKeys(groupingKeys).mkString("\n")}
       |
       |        ${buffVars.map(_.code).mkString("\n")}
       |
       |        // Initialize aggregate values
       |        ${genCodeToSetAggBuffers(bufferValues).mkString("\n")}
       |
       |        buckets[idx] = numRows++;
       |        batch.setNumRows(numRows);
       |        aggregateBufferBatch.setNumRows(numRows);
       |        return aggregateBufferBatch.getRow(buckets[idx]);
       |      } else {
       |        // No more space
       |        return null;
       |      }
       |    } else if (equals(idx, ${groupingKeys.map(_.name).mkString(", ")})) { //filled, check equality
       |      return aggregateBufferBatch.getRow(buckets[idx]);
       |    }
       |    // move on to the next position
       |    idx = (idx + 1) & (numBuckets - 1);
       |    step++;
       |  }
       |  // Didn't find it
       |  return null;
       |}
     """.stripMargin
  }

  private def generateRowIterator(): String = {
    s"""
       |public java.util.Iterator<org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row>
       |    rowIterator() {
       |  return batch.rowIterator();
       |}
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
