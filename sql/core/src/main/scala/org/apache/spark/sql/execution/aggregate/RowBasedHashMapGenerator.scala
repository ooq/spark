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
 * This is a helper class to generate an append-only row-based hash map that can act as a 'cache'
 * for extremely fast key-value lookups while evaluating aggregates (and fall back to the
 * `BytesToBytesMap` if a given key isn't found). This is 'codegened' in HashAggregate to speed
 * up aggregates w/ key.
 *
 * We also have VectorizedHashMapGenerator, which generates a append-only vectorized hash map.
 * We choose one of the two as the 1st level, fast hash map during aggregation.
 *
 * NOTE: This row-based hash map currently doesn't support nullable keys and falls back to the
 * `BytesToBytesMap` to store them.
 */
class RowBasedHashMapGenerator(
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
       |public class $generatedClassName {
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
       |  private org.apache.spark.sql.execution.vectorized.ColumnarBatch batch;
       |  private org.apache.spark.sql.execution.vectorized.ColumnarBatch aggregateBufferBatch;
       |  private int[] buckets;
       |  private int capacity = 1 << 16;
       |  private double loadFactor = 0.5;
       |  private int numBuckets = (int) (capacity / loadFactor);
       |  private int maxSteps = 2;
       |  private int numRows = 0;
       |  private org.apache.spark.sql.types.StructType schema = $generatedSchema
       |  private org.apache.spark.sql.types.StructType aggregateBufferSchema =
       |    $generatedAggBufferSchema
       |
       |  public $generatedClassName() {
       |    batch = org.apache.spark.sql.execution.vectorized.ColumnarBatch.allocate(schema,
       |      org.apache.spark.memory.MemoryMode.ON_HEAP, capacity);
       |    // TODO: Possibly generate this projection in HashAggregate directly
       |    aggregateBufferBatch = org.apache.spark.sql.execution.vectorized.ColumnarBatch.allocate(
       |      aggregateBufferSchema, org.apache.spark.memory.MemoryMode.ON_HEAP, capacity);
       |    for (int i = 0 ; i < aggregateBufferBatch.numCols(); i++) {
       |       aggregateBufferBatch.setColumn(i, batch.column(i+${groupingKeys.length}));
       |    }
       |
       |    buckets = new int[numBuckets];
       |    java.util.Arrays.fill(buckets, -1);
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
   * associated [[org.apache.spark.sql.catalyst.expressions.RowBatch]]. For instance, if we
   * have 2 long group-by keys, the generated function would be of the form:
   *
   * {{{
   * private boolean equals(int idx, long agg_key, long agg_key1) {
   *   return batch.row(buckets[idx]).getLong(0) == agg_key &&
   *     batch.row(buckets[idx]).getLong(1) == agg_key1;
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
   * Generates a method that returns a
   * [[org.apache.spark.sql.catalyst.expressions.UnsafeRow]] which keeps track of the
   * aggregate value(s) for a given set of keys. If the corresponding row doesn't exist, the
   * generated method adds the corresponding row in the associated
   * [[org.apache.spark.unsafe.memory.MemoryBlock]].
   *
   * TODO: add an example instance
   *
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
       |public org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row findOrInsert(${
            groupingKeySignature}) {
       |  long h = hash(${groupingKeys.map(_.name).mkString(", ")});
       |  int step = 0;
       |  int idx = (int) h & (numBuckets - 1);
       |  while (step < maxSteps) {
       |    // Return bucket index if it's either an empty slot or already contains the key
       |    if (buckets[idx] == -1) {
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
       |    } else if (equals(idx, ${groupingKeys.map(_.name).mkString(", ")})) {
       |      return aggregateBufferBatch.getRow(buckets[idx]);
       |    }
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
