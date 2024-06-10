/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.btree.BTree;

import static org.junit.Assert.assertEquals;

public class RowFilterTest
{
    @Test
    public void testCQLFilterClose()
    {
        // CASSANDRA-15126
        TableMetadata metadata = TableMetadata.builder("testks", "testcf")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addStaticColumn("s", Int32Type.instance)
                                              .addRegularColumn("r", Int32Type.instance)
                                              .offline()
                                              .build();
        ColumnMetadata s = metadata.getColumn(new ColumnIdentifier("s", true));
        ColumnMetadata r = metadata.getColumn(new ColumnIdentifier("r", true));

        ByteBuffer one = Int32Type.instance.decompose(1);
        RowFilter filter = RowFilter.none().withNewExpressions(new ArrayList<>());
        filter.add(s, Operator.NEQ, one);
        AtomicBoolean closed = new AtomicBoolean();
        UnfilteredPartitionIterator iter = filter.filter(new SingletonUnfilteredPartitionIterator(new UnfilteredRowIterator()
        {
            public DeletionTime partitionLevelDeletion() { return null; }
            public EncodingStats stats() { return null; }
            public TableMetadata metadata() { return metadata; }
            public boolean isReverseOrder() { return false; }
            public RegularAndStaticColumns columns() { return null; }
            public DecoratedKey partitionKey() { return null; }
            public boolean hasNext() { return false; }
            public Unfiltered next() { return null; }
            public Row staticRow()
            {
                return BTreeRow.create(Clustering.STATIC_CLUSTERING,
                                       LivenessInfo.EMPTY,
                                       Row.Deletion.LIVE,
                                       BTree.singleton(new BufferCell(s, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME, one, null)));
            }
            public void close()
            {
                closed.set(true);
            }
        }), 1);
        Assert.assertFalse(iter.hasNext());
        Assert.assertTrue(closed.get());

        filter = RowFilter.none().withNewExpressions(new ArrayList<>());
        filter.add(r, Operator.NEQ, one);
        closed.set(false);
        iter = filter.filter(new SingletonUnfilteredPartitionIterator(new UnfilteredRowIterator()
        {
            boolean hasNext = true;
            public DeletionTime partitionLevelDeletion() { return null; }
            public EncodingStats stats() { return null; }
            public TableMetadata metadata() { return metadata; }
            public boolean isReverseOrder() { return false; }
            public RegularAndStaticColumns columns() { return null; }
            public DecoratedKey partitionKey() { return null; }
            public Row staticRow() { return Rows.EMPTY_STATIC_ROW; }
            public boolean hasNext()
            {
                boolean r = hasNext;
                hasNext = false;
                return r;
            }
            public Unfiltered next()
            {
                return BTreeRow.create(Clustering.EMPTY,
                                       LivenessInfo.EMPTY,
                                       Row.Deletion.LIVE,
                                       BTree.singleton(new BufferCell(r, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME, one, null)));
            }
            public void close()
            {
                closed.set(true);
            }
        }), 1);
        Assert.assertFalse(iter.hasNext());
        Assert.assertTrue(closed.get());
    }
    @Test
    public void testExpressionSerializationWithUnaryOperator() throws IOException
    {
        TableMetadata metadata = TableMetadata.builder("ks", "tbl")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addRegularColumn("r", Int32Type.instance)
                                              .addRegularColumn("m1", MapType.getInstance(Int32Type.instance, Int32Type.instance, true))
                                              .addRegularColumn("m2", MapType.getInstance(Int32Type.instance, Int32Type.instance, false))
                                              .offline()
                                              .build();

//        ColumnMetadata simpleColumn = metadata.getColumn(new ColumnIdentifier("r", true));
//        ColumnMetadata unfrozenMapColumn = metadata.getColumn(new ColumnIdentifier("m1", true))
        ColumnMetadata frozenMapColumn = metadata.getColumn(new ColumnIdentifier("m2", true));

        RowFilter rowFilter = RowFilter.create(false);
//        rowFilter.add(simpleColumn, Operator.IS_NOT_NULL, null);
//        rowFilter.addMapElementExpression(unfrozenMapColumn, Int32Type.instance.decompose(1), Operator.IS_NOT_NULL, null);
        rowFilter.addMapElementExpression(frozenMapColumn, Int32Type.instance.decompose(1),Operator.IS_NOT_NULL, null);

        long size = RowFilter.serializer.serializedSize(rowFilter, MessagingService.current_version);
        DataOutputBuffer dataOutput = new DataOutputBuffer(ByteBuffer.allocate((int) size));
        RowFilter.serializer.serialize(rowFilter, dataOutput, MessagingService.current_version);
        DataInputBuffer dataInput = new DataInputBuffer(dataOutput.buffer(), false);

        RowFilter deserialized = RowFilter.serializer.deserialize(dataInput, MessagingService.current_version, metadata, false);
        assertEquals(rowFilter, deserialized);
    }
    @Test
    public void testToString()
    {
        ColumnMetadata simple = ColumnMetadata.regularColumn("ks", "tbl", "v", Int32Type.instance);
        ColumnMetadata unfrozenMap = ColumnMetadata.regularColumn("ks", "tbl", "m1", MapType.getInstance(Int32Type.instance, Int32Type.instance, true));
        ColumnMetadata frozenMap = ColumnMetadata.regularColumn("ks", "tbl", "m2", MapType.getInstance(Int32Type.instance, Int32Type.instance, true));

        ByteBuffer decompose = Int32Type.instance.decompose(1);

        RowFilter rowFilter = RowFilter.create(false);
        rowFilter.add(simple, Operator.EQ, decompose);
        rowFilter.add(simple, Operator.IS_NOT_NULL, null);
        rowFilter.addMapElementExpression(unfrozenMap, decompose, Operator.IS_NOT_NULL, null);
        rowFilter.addMapElementExpression(frozenMap, decompose, Operator.IS_NOT_NULL, null);

        assertEquals("v = 1 AND v IS NOT NULL AND m1[1] IS NOT NULL AND m2[1] IS NOT NULL", rowFilter.toString());
        assertEquals("v = 1 AND v IS NOT NULL AND m1[1] IS NOT NULL AND m2[1] IS NOT NULL", rowFilter.toCQLString());
    }
}
