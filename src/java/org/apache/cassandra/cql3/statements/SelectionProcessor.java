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

package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.selection.ResultSetBuilder;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.db.aggregation.GroupMaker;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.schema.TableMetadata;

public class SelectionProcessor implements RowProcessor<ResultSet>
{
    private final TableMetadata table;

    private final ResultSet.ResultMetadata resultMetadata;

    private final Selection.Selectors selectors;

    private final long nowInSec;

    private final boolean returnStaticContentOnPartitionWithNoRows;

    private final boolean unmask;

    private final GroupMaker groupMaker;

    private final Comparator<List<ByteBuffer>> comparator;

    private final int userLimit;

    private final ResultSetBuilder.Listener listener;

    public static Builder newBuilder(TableMetadata table, Selection selection, QueryOptions options, long nowInSec)
    {
        return new Builder(table, selection.getResultMetadata(), selection.newSelectors(options), nowInSec);
    }

    public static Builder newBuilder(TableMetadata table, ResultSet.ResultMetadata resultMetadata, Selection.Selectors selectors, long nowInSec)
    {
        return new Builder(table, resultMetadata, selectors, nowInSec);
    }

    private SelectionProcessor(Builder builder)
    {
        this.table = builder.table;
        this.resultMetadata = builder.resultMetadata;
        this.selectors = builder.selectors;
        this.nowInSec = builder.nowInSec;
        this.returnStaticContentOnPartitionWithNoRows = builder.returnStaticContentOnPartitionWithNoRows;
        this.unmask = builder.unmask;
        this.groupMaker = builder.groupMaker;
        this.comparator = builder.comparator;
        this.userLimit = builder.userLimit;
        this.listener = builder.listener;
    }

    public ResultSet.ResultMetadata getResultMetadata()
    {
        return resultMetadata;
    }

    @Override
    public ResultSet process(PartitionIterator partitions)
    {
        ResultSetBuilder result = new ResultSetBuilder(resultMetadata, selectors, unmask, groupMaker, listener);

        while (partitions.hasNext())
        {
            try (RowIterator partition = partitions.next())
            {
                processPartition(partition, result, nowInSec);
            }
        }

        ResultSet cqlRows = result.build();

        if (!cqlRows.isEmpty() && comparator != null)
            cqlRows.rows.sort(comparator);

        cqlRows.trim(userLimit);

        return cqlRows;
    }

    private void processPartition(RowIterator partition, ResultSetBuilder result, long nowInSec)
    {
        ByteBuffer[] keyComponents = table.partitionKeyComponents(partition.partitionKey());

        Row staticRow = partition.staticRow();
        // If there is no rows, we include the static content if we should and we're done.
        if (!partition.hasNext())
        {
            if (!staticRow.isEmpty() && returnStaticContentOnPartitionWithNoRows)
            {
                result.addStaticRow(partition.partitionKey(), keyComponents, staticRow, nowInSec);
            }
            return;
        }

        while (partition.hasNext())
        {
            result.addRow(partition.partitionKey(), keyComponents, staticRow, partition.next(), nowInSec);
        }
    }

    public static class Builder
    {
        private final TableMetadata table;

        private final ResultSet.ResultMetadata resultMetadata;

        private final Selection.Selectors selectors;

        private final long nowInSec;

        private boolean returnStaticContentOnPartitionWithNoRows;

        private boolean unmask;

        private GroupMaker groupMaker;

        private Comparator<List<ByteBuffer>> comparator;

        private int userLimit = DataLimits.NO_LIMIT;

        private ResultSetBuilder.Listener listener = ResultSetBuilder.Listener.NOOP;

        public Builder(TableMetadata table, ResultSet.ResultMetadata resultMetadata, Selection.Selectors selectors, long nowInSec)
        {
            this.table = table;
            this.resultMetadata = resultMetadata;
            this.selectors = selectors;
            this.nowInSec = nowInSec;
        }

        public Builder returnStaticContentOnPartitionWithNoRows(boolean returnStaticContentOnPartitionWithNoRows)
        {
            this.returnStaticContentOnPartitionWithNoRows = returnStaticContentOnPartitionWithNoRows;
            return this;
        }

        public Builder unmask(boolean unmask)
        {
            this.unmask = unmask;
            return this;
        }

        public Builder groupMaker(GroupMaker groupMaker)
        {
            this.groupMaker = groupMaker;
            return this;
        }

        public Builder comparator(Comparator<List<ByteBuffer>> comparator)
        {
            this.comparator = comparator;
            return this;
        }

        public Builder userLimit(int userLimit)
        {
            this.userLimit = userLimit;
            return this;
        }

        public Builder listener(ResultSetBuilder.Listener listener)
        {
            this.listener = listener;
            return this;
        }

        public SelectionProcessor build()
        {
            return new SelectionProcessor(this);
        }
    }
}
