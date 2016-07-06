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

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
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
  val foundKeys = groupingKeySchema.map(k => Buffer(k.dataType, ctx.freshName("foundkey")))
  val foundKeySignature =
    foundKeys.map(key => s"${ctx.javaType(key.dataType)} ${key.name}").mkString(", ")

  val keyBufferName = "currentKeyBuffer"
  //val foundKeyAssignment = foundKeys.zipWithIndex.map { case (key: Buffer, ordinal: Int) =>
  //  s"${key.name} = ${ctx.getValue(keyBufferName, key.dataType, ordinal.toString())}"}
  //  .mkString(";\n")

  val foundKeyAssignment = foundKeys.zipWithIndex.map { case (key: Buffer, ordinal: Int) =>
    s"${key.name} = Platform.getLong(foundBase, foundOff + ${(ordinal*8+8).toString()})"}
    .mkString(";\n")

  val numVarLenFields = groupingKeys.map(_.dataType).count {
    case dt if UnsafeRow.isFixedLength(dt) => false
    // TODO: consider large decimal and interval type
    case _ => true
  }

  val createUnsafeRowForKey = groupingKeys.zipWithIndex.map { case (key: Buffer, ordinal: Int) =>
    s"agg_rowWriter.write(${ordinal}, ${key.name})"}
    .mkString(";\n")


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
       |
       |public class $generatedClassName extends MemoryConsumer{
       |${initializeAggregateHashMap()}
       |
       |${generateFindOrInsert()}
       |
       |${generateInsert()}
       |
       |${generateAcquireNewPage()}
       |
       |${generateArrayEquals()}
       |
       |${generateKeyEquals()}
       |
       |${generateHashFunction()}
       |
       |${generateKVIterator()}
       |
       |${generateSpill()}
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
       |  private int capacity = 1 << 20;
       |  private double loadFactor = 0.5;
       |  private int maxSteps= 1 << 20;
       |  private long totalAdditionalProbs = 0;
       |  private int numRows = 0;
       |  private org.apache.spark.sql.types.StructType schema = $generatedSchema
       |  private org.apache.spark.sql.types.StructType aggregateBufferSchema =
       |    $generatedAggBufferSchema
       |  private org.apache.spark.sql.types.StructType groupingKeySchema =
       |    $generatedGroupingKeySchema
       |  private long[] longArray;
       |  private int mask;
       |  private TaskMemoryManager taskMemoryManager;
       |  private final java.util.LinkedList<MemoryBlock> dataPages = new java.util.LinkedList<MemoryBlock>();
       |  private MemoryBlock currentPage = null;
       |  private long pageCursor = 0;
       |  private final byte[] emptyAggregationBuffer;
       |
       |  private Object vbase;
       |  private long voff;
       |  private int vlen;
       |
       |  private long foundFullKeyAddress;
       |  private Object foundBase;
       |  private long foundOff;
       |  private int foundLen;
       |  private int foundTotalLen;
       |
       |  // a re-used pointer to the current aggregation buffer
       |  private final UnsafeRow currentAggregationBuffer;
       |  private boolean isPointed = false;
       |  private final UnsafeRow currentKeyBuffer;
       |
       |
       |
       |  ${foundKeys.map(key => "private " + s"${ctx.javaType(key.dataType)}" + " " + key.name).mkString(";\n")};
       |
       |  public $generatedClassName(
       |    TaskMemoryManager taskMemoryManager,
       |    InternalRow emptyAggregationBuffer) {
       |    super(taskMemoryManager,
       |      taskMemoryManager.pageSizeBytes(),
       |      taskMemoryManager.getTungstenMemoryMode());
       |    this.taskMemoryManager = taskMemoryManager;
       |    longArray = new long[capacity *2 * 2];
       |    java.util.Arrays.fill(longArray, 0);
       |    mask = capacity*2 - 1;
       |
       |    final UnsafeProjection valueProjection = UnsafeProjection.create(aggregateBufferSchema);
       |    this.emptyAggregationBuffer = valueProjection.apply(emptyAggregationBuffer).getBytes();
       |
       |    this.currentAggregationBuffer = new UnsafeRow(aggregateBufferSchema.length());
       |    this.currentKeyBuffer = new UnsafeRow(groupingKeySchema.length());
       |
       |    vbase = this.emptyAggregationBuffer;
       |    voff = Platform.BYTE_ARRAY_OFFSET;
       |    vlen = this.emptyAggregationBuffer.length;
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

  private def generateMurmurHashFunction(): String = {
    val hash = ctx.freshName("hash")

    def genHashForKeys(groupingKeys: Seq[Buffer]): String = {
      groupingKeys.map { key =>
        val result = ctx.freshName("result")
        s"""
           |${genComputeHash(ctx, key.name, key.dataType, result)}
           |$hash = org.apache.spark.unsafe.hash.Murmur3_x86_32.hashLong($result, $hash);
          """.stripMargin
      }.mkString("\n")
    }

    s"""
       |private int hash($groupingKeySignature) {
       |  int $hash = 42;
       |  ${genHashForKeys(groupingKeys)}
       |  return $hash;
       |}
     """.stripMargin
  }

  private def generateKeyEquals(): String = {
    def genEqualsForKeys(groupingKeys: Seq[Buffer], foundKeys: Seq[Buffer]): String = {
      val gk = groupingKeys.zipWithIndex.map { case (key: Buffer, ordinal: Int) =>
        (ordinal, key.name)}.toSeq
      val fk = foundKeys.zipWithIndex.map { case (key: Buffer, ordinal: Int) =>
        (ordinal, key.name)}.toSeq
      val join = (gk ++ fk)
        .groupBy(_._1).mapValues(_.map(_._2).toList)
        .map(a => a._2(0) + " == " + a._2(1)).mkString(" && ")
      join
    }
    def printAllKeys(groupingKeys: Seq[Buffer], foundKeys: Seq[Buffer]): String = {
      val printGroupingKeys =
        groupingKeys.map(key => s"System.out.println(${key.name})").mkString(";\n")
      val printFoundKeys =
        foundKeys.map(key => s"System.out.println(${key.name})").mkString(";\n")
      s"${printGroupingKeys};\n${printFoundKeys}"
    }

    s"""
       |private boolean keyEquals($groupingKeySignature, $foundKeySignature) {
       |  return (${genEqualsForKeys(groupingKeys, foundKeys)});
       |
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
       |    //System.out.println("acquired a new page");
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
       |//public UnsafeRow findOrInsert(UnsafeRow rowKey,
       |public UnsafeRow findOrInsert(
       |${groupingKeySignature}) {
       |  
       |  //long h = -1640531527L;
       |  int h = (int)hash(${groupingKeys.map(_.name).mkString(", ")});
       |  //int h = (int) agg_key;
       |
       |  //System.out.println("" + agg_key + ":" + h);
       |  int step = 0;
       |  int pos = (int) h & mask;
       |  //System.out.println("print rowkey ------");
       |  //System.out.println(rowKey);
       |  ${groupingKeys.map("//System.out.println(" + _.name + ");").mkString("\n")}
       |  //System.out.println("end print ------");
       |  //Object kbase = rowKey.getBaseObject(); // could be cogen
       |  //long koff = rowKey.getBaseOffset();  // could be cogen
       |  //int klen = rowKey.getSizeInBytes(); // could be cogen
       |  while (step < maxSteps) {
       |    if (longArray[pos * 2] == 0) { //new entry
       |      return insert(${groupingKeys.map(_.name).mkString(", ")}, pos, h);
       |    } else {
       |      // we will check equality here and return buffer if matched
       |      // 1st level: hash code match
       |      //System.out.println("cache hit");
       |      //if ((int)stored == h) {
       |          // 2nd level: keys match
       |          // TODO: codegen based with key types, so we don't need byte by byte compare
       |          foundFullKeyAddress = longArray[pos * 2];
       |          //System.out.println(foundFullKeyAddress);
       |          foundBase = taskMemoryManager.getPage(foundFullKeyAddress);
       |          foundOff = taskMemoryManager.getOffsetInPage(foundFullKeyAddress) + 8;
       |          //System.out.println(foundOff);
       |          foundLen = Platform.getInt(foundBase, foundOff-4);
       |          //System.out.println(foundLen);
       |          foundTotalLen = Platform.getInt(foundBase, foundOff-8);
       |          //System.out.println(foundTotalLen);
       |          
       |          //Object foundBase = ((MemoryBlock)dataPages.peek()).getBaseObject();
       |          //long foundOff = 28;
       |          //foundLen = 16;
       |          //foundTotalLen = 36;
       |          
       |          //System.out.println("here");
       |          //if (foundLen == klen) {
       |              //if (arrayEquals(kbase, koff, foundBase, foundOff, klen)) {
       |
       |
       |              currentKeyBuffer.pointTo(foundBase, foundOff, foundLen);
       |              ${foundKeyAssignment};
       |
       |              //long agg_foundkey = Platform.getLong(foundBase, foundOff + 8); //HACK
       |              if(keyEquals(${groupingKeys.map(_.name).mkString(", ")},
       |                ${foundKeys.map(_.name).mkString(", ")})) {
       |              //System.out.println("complete match");
       |              //UnsafeRow currentAggregationBuffer = new UnsafeRow(1);
       |              currentAggregationBuffer.pointTo(foundBase, foundOff + foundLen, foundTotalLen - foundLen);
       |	            //isPointed = true;
       |              //totalAdditionalProbs += step;
       |              return currentAggregationBuffer;
       |            }
       |         // }
       |      }
       |
       |    //}
       |    // move on to the next position
       |    // TODO: change the strategies
       |    // now triangle probing
       |    //System.out.println("one miss");
       |    step++;
       |    //pos = (pos + 1) & mask; // linear probing
       |    pos = (pos + step) & mask; // triangular probing
       |  }
       |  // Didn't find it
       |  //System.err.println("Did not find it with max retries");
       |  return null;
       |}
     """.stripMargin
  }

  private def generateInsert(): String = {

    s"""
       |private UnsafeRow insert(
       |${groupingKeySignature}, int pos, int h) {
       |
       |      if (numRows < capacity) {
       |
       |
       |        // creating the unsafe for new entry
       |        UnsafeRow agg_result = new UnsafeRow(${groupingKeySchema.length});
       |        BufferHolder agg_holder = new BufferHolder(agg_result, ${numVarLenFields * 32});
       |        UnsafeRowWriter agg_rowWriter = new UnsafeRowWriter(agg_holder, ${groupingKeySchema.length});
       |
       |        //TODO: handling nullable types
       |        agg_holder.reset();
       |        agg_rowWriter.zeroOutNullBytes();
       |
       |        ${createUnsafeRowForKey};
       |
       |        agg_result.setTotalSize(agg_holder.totalSize());
       |
       |        Object kbase = agg_result.getBaseObject(); // could be cogen
       |        long koff = agg_result.getBaseOffset();  // could be cogen
       |        int klen = agg_result.getSizeInBytes(); // could be cogen
       |
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
       |        //System.out.println("initialize aggregate keys");
       |        // Initialize aggregate keys
       |        // append the keys and values to current data page
       |        final Object base = currentPage.getBaseObject();
       |        long offset = currentPage.getBaseOffset() + pageCursor;
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
       |        //System.out.println("Update bookkeeping data structures");
       |        // Update bookkeeping data structures -----------------------------
       |        offset = currentPage.getBaseOffset();
       |        Platform.putInt(base, offset, Platform.getInt(base, offset) + 1);
       |        pageCursor += recordLength;
       |        final long storedKeyAddress = taskMemoryManager.encodePageNumberAndOffset(
       |        currentPage, recordOffset);
       |        longArray[pos * 2] =  storedKeyAddress;
       |        longArray[pos * 2 + 1] = h;
       |        numRows++;
       |
       |        // now we want point the value UnsafeRow to the correct location
       |        // basically valuebase, valueoffset and value length
       |        currentAggregationBuffer.pointTo(base, recordOffset + 8 + klen, vlen);
       |        //System.out.println("returning value buffer");
       |
       |        return currentAggregationBuffer;
       |      } else {
       |        // No more space
       |        System.err.println("No more space");
       |        return null;
       |      }
       |}
     """.stripMargin
  }

  private def generateKVIterator(): String = {
    s"""
       |
       |  public org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow> iterator() {
       |    return new org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow>() {
       |
       |      private final UnsafeRow key = new UnsafeRow(groupingKeySchema.length());
       |      private final UnsafeRow value = new UnsafeRow(aggregateBufferSchema.length());
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
       |      private boolean inited = false;
       |
       |
       |      private void init() {
       |        if (dataPages.size() > 0) {
       |          currentPage = (MemoryBlock) dataPages.remove();
       |          pageBaseObject = currentPage.getBaseObject();
       |          offsetInPage = currentPage.getBaseOffset();
       |          recordsInPage = Platform.getInt(pageBaseObject, offsetInPage);
       |          offsetInPage += 4;
       |        }
       |        inited = true;
       |      }
       |
       |      @Override
       |      public boolean next() {
       |        if (!inited) init();
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
       |        value.pointTo(pageBaseObject, offsetInPage + 8 + klen, vlen);
       |        offsetInPage += 4 + totalLength + 8;
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
       |        System.out.println("Number of rows: " + numRows);
       |        System.out.println("Avg additional probes: " + totalAdditionalProbs);
       |        // Do nothing.
       |      }
       |
       |      private boolean advanceToNextPage() {
       |        if (dataPages.size() > 0) {
       |          currentPage = (MemoryBlock) dataPages.remove();
       |          pageBaseObject = currentPage.getBaseObject();
       |          offsetInPage = currentPage.getBaseOffset();
       |          recordsInPage = Platform.getInt(pageBaseObject, offsetInPage);
       |          offsetInPage += 4;
       |          return true;
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
       |  //do nothing
       |}
     """.stripMargin
  }

  private def generateSpill(): String = {
    s"""
       |public long spill(long size, MemoryConsumer trigger) throws IOException {
       |  System.err.println("We expect spilling never happens");
       |  return 0L;
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
