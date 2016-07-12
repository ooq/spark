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
package org.apache.spark.sql.catalyst.expressions;

import java.math.BigDecimal;
import java.util.*;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow;
import org.apache.spark.sql.catalyst.expressions.MutableRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;


/**
 * RowBatch
 *
 * TODO: making each entry more compact, e.g., combine key and value into a single UnsafeRow
 */
public final class RowBatch {
    private static final int DEFAULT_CAPACITY = 4 * 1024;

    private final TaskMemoryManager taskMemoryManager;

    private final StructType keySchema;
    private final StructType valueSchema;
    private final int capacity;
    private int numRows = 0;

    // Staging row returned from getRow.
    final UnsafeRow keyRow;
    final UnsafeRow valueRow;

    private int keyRowId = -1;
    private int valueRowId = -1;

    private long[] keyFullAddress;
    private long[] valueFullAddress;
    // Shortcuts for lengths, which can also be retrieved directly from UnsafeRow
    // TODO: might want to remove this shortcut, retrieving directly from UnsafeRow might be
    // fast due to cache locality
    private int[] keyLength;
    private int[] valueLength;

    private final LinkedList<MemoryBlock> dataPages = new LinkedList<>();

    public static RowBatch allocate(StructType keySchema, StructType valueSchema,
                                    TaskMemoryManager manager) {
        return new RowBatch(keySchema, valueSchema, DEFAULT_CAPACITY, manager);
    }

    public static RowBatch allocate(StructType keySchema, StructType valueSchema,
                                    TaskMemoryManager manager, int maxRows) {
        return new RowBatch(keySchema, valueSchema, maxRows, manager);
    }

    public int numRows() { return numRows; }

    public void close() {
    }

    private void assertAndCreateIfNotExist(int rowId) {
        assert(rowId >= 0);
        assert(rowId <= numRows);
        if (rowId == numRows) {
            // creating a new row

            numRows++;
        }
    }
    /**
     * Returns the key row in this batch at `rowId`. Returned key row is reused across calls.
     */
    public UnsafeRow getKeyRow(int rowId) {
        assertAndCreateIfNotExist(rowId);
        if (keyRowId != rowId) {
            long fullAddress = keyFullAddress[rowId];
            //TODO: if decoding through manager
            //is not fast enough, we can do a simple version in this class
            Object base = taskMemoryManager.getPage(fullAddress);
            long offset = taskMemoryManager.getOffsetInPage(fullAddress);
            keyRow.pointTo(base, offset, keyLength[rowId]);
            keyRowId = rowId;
        }
        return keyRow;
    }

    /**
     * Returns the value row in this batch at `rowId`. Returned value row is reused across calls.
     */
    public UnsafeRow getValueRow(int rowId) {
        assertAndCreateIfNotExist(rowId);
        if (valueRowId != rowId) {
            long fullAddress = valueFullAddress[rowId];
            //TODO: if decoding through manager
            //is not fast enough, we can do a simple version in this class
            Object base = taskMemoryManager.getPage(fullAddress);
            long offset = taskMemoryManager.getOffsetInPage(fullAddress);
            valueRow.pointTo(base, offset, valueLength[rowId]);
            valueRowId = rowId;
        }
        return valueRow;
    }

    /**
     * Returns the value row in this batch at `rowId`.
     * It can be a faster path if `keyRowId` is equal to `rowId`, which means the preceding
     * key row has just been accessed. As this is often the case, this method should be preferred
     * over `getValueRow()`.
     * This method is faster than `getValueRow()` because it avoids address decoding, instead reuse
     * the page and offset information from the preceding key row.
     * Returned value row is reused across calls.
     */
    public UnsafeRow getValueFromKey(int rowId) {
        assert(rowId >= 0);
        if (keyRowId != rowId) {
            getKeyRow(rowId);
        }
        valueRow.pointTo(keyRow.getBaseObject(),
                keyRow.getBaseOffset() + keyLength[rowId],
                valueLength[rowId]);
        valueRowId = rowId;
        return valueRow;
    }


    private RowBatch(StructType keySchema, StructType valueSchema, int maxRows,
                     TaskMemoryManager manager) {
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.capacity = maxRows;
        this.taskMemoryManager = manager;
        this.keyFullAddress = new long[maxRows];
        this.valueFullAddress = new long[maxRows];
        this.keyLength = new int[maxRows];
        this.valueLength = new int[maxRows];

        this.keyRow = new UnsafeRow(keySchema.length());
        this.valueRow = new UnsafeRow(valueSchema.length());
    }
}
