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

package org.apache.cassandra.cql3;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.cql3.terms.Constants;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ComparableTestUtils;

public class ColumnsExpressionTest
{
    @Test
    public void testCompareTo()
    {
        TableMetadata table = TableMetadata.builder("ks", "tbl")
                                           .partitioner(Murmur3Partitioner.instance)
                                           .addPartitionKeyColumn("pk1", UTF8Type.instance)
                                           .addPartitionKeyColumn("pk2", Int32Type.instance)
                                           .addClusteringColumn("c1", Int32Type.instance)
                                           .addClusteringColumn("c2", Int32Type.instance)
                                           .addStaticColumn("s", UTF8Type.instance)
                                           .addRegularColumn("r", UTF8Type.instance)
                                           .addRegularColumn("m", MapType.getInstance(UTF8Type.instance,
                                                                                            UTF8Type.instance,
                                                                                            true))
                                           .build();

        Constants.Literal key = Constants.Literal.string("key");

        ComparableTestUtils.assertOrder(token(table, "pk1", "pk2"),
                                        simpleColumn(table, "pk1"),
                                        simpleColumn(table, "pk2"),
                                        multiColumns(table, "c1", "c2"),
                                        simpleColumn(table, "c1"),
                                        multiColumns(table, "c2"),
                                        simpleColumn(table, "c2"),
                                        simpleColumn(table, "s"),
                                        simpleColumn(table, "m"),
                                        mapElement(table, "m", key),
                                        simpleColumn(table, "r"));
    }

    private static ColumnsExpression simpleColumn(TableMetadata table, String column)
    {
        return ColumnsExpression.Raw.singleColumn(new ColumnIdentifier(column, true)).prepare(table);
    }

    private static ColumnsExpression token(TableMetadata table, String... columns)
    {
        return ColumnsExpression.Raw.token(toIdentifiers(columns)).prepare(table);
    }

    private static ColumnsExpression multiColumns(TableMetadata table, String... columns)
    {
        return ColumnsExpression.Raw.multiColumns(toIdentifiers(columns)).prepare(table);
    }

    private static ColumnsExpression mapElement(TableMetadata table, String column, Term.Raw key)
    {
        return ColumnsExpression.Raw.mapElement(new ColumnIdentifier(column, true), key).prepare(table);
    }

    private static List<ColumnIdentifier> toIdentifiers(String[] columns)
    {
        return Arrays.stream(columns)
                     .map(c -> new ColumnIdentifier(c, true))
                     .collect(Collectors.toList());
    }
}
