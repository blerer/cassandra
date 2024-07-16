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
package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.ResultSet.ResultMetadata;
import org.apache.cassandra.cql3.selection.Selection.Selectors;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.aggregation.GroupMaker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;

public final class ResultSetBuilder
{
    private final ResultSet resultSet;

    /**
     * As multiple thread can access a <code>Selection</code> instance each <code>ResultSetBuilder</code> will use
     * its own <code>Selectors</code> instance.
     */
    private final Selectors selectors;

    /**
     * The <code>GroupMaker</code> used to build the aggregates.
     */
    private final GroupMaker groupMaker;

    /**
     * Whether masked columns should be unmasked.
     */
    private final boolean unmask;

    /*
     * We'll build CQL3 row one by one.
     */
    private Selector.InputRow inputRow;

    private long sizeInBytes;
    private boolean sizeWarningEmitted = false;

    public ResultSetBuilder(ResultMetadata metadata, Selectors selectors, boolean unmask)
    {
        this(metadata, selectors, unmask, null);
    }

    public ResultSetBuilder(ResultMetadata metadata, Selectors selectors, boolean unmask, GroupMaker groupMaker)
    {
        this.resultSet = new ResultSet(metadata.copy(), new ArrayList<>());
        this.selectors = selectors;
        this.groupMaker = groupMaker;
        this.unmask = unmask;
    }

    private void addSize(List<ByteBuffer> row)
    {
        for (int i = 0, m = row.size(); i < m; i++)
        {
            ByteBuffer value = row.get(i);
            sizeInBytes += value != null ? value.remaining() : 0;
        }
    }

    public boolean shouldWarn(long thresholdBytes)
    {
        if (thresholdBytes != -1 && !sizeWarningEmitted && sizeInBytes > thresholdBytes)
        {
            sizeWarningEmitted = true;
            return true;
        }
        return false;
    }

    public boolean shouldReject(long thresholdBytes)
    {
        return thresholdBytes != -1 && sizeInBytes > thresholdBytes;
    }

    public long sizeInBytes()
    {
        return sizeInBytes;
    }


    public void addStaticRow(DecoratedKey partitionKey, ByteBuffer[] pratitionKeyComponents, Row staticRow, long nowInSec)
    {
        newRow(partitionKey, staticRow.clustering(), selectors.columns());

        for (ColumnMetadata def : selectors.columns())
        {
            switch (def.kind)
            {
                case PARTITION_KEY:
                    inputRow.add(pratitionKeyComponents[def.position()]);
                    break;
                case STATIC:
                    inputRow.add(staticRow.getColumnData(def), nowInSec);
                    break;
                default:
                    inputRow.add(null);
            }
        }
    }

    public void addRow(DecoratedKey partitionKey, ByteBuffer[] pratitionKeyComponents, Row staticRow, Row row, long nowInSec)
    {
        newRow(partitionKey, row.clustering(), selectors.columns());

        // Respect selection order
        for (ColumnMetadata def : selectors.columns())
        {
            switch (def.kind)
            {
                case PARTITION_KEY:
                    inputRow.add(pratitionKeyComponents[def.position()]);
                    break;
                case CLUSTERING:
                    inputRow.add(row.clustering().bufferAt(def.position()));
                    break;
                case REGULAR:
                    inputRow.add(row.getColumnData(def), nowInSec);
                    break;
                case STATIC:
                    inputRow.add(staticRow.getColumnData(def), nowInSec);
                    break;
            }
        }
    }

    /**
     * Notifies this <code>Builder</code> that a new row is being processed.
     *
     * @param partitionKey the partition key of the new row
     * @param clustering the clustering of the new row
     */
    public void newRow(DecoratedKey partitionKey, Clustering<?> clustering, List<ColumnMetadata> columns)
    {
        // The groupMaker needs to be called for each row
        boolean isNewAggregate = groupMaker == null || groupMaker.isNewGroup(partitionKey, clustering);
        if (inputRow != null)
        {
            selectors.addInputRow(inputRow);
            if (isNewAggregate)
            {
                resultSet.addRow(getOutputRow());
                inputRow.reset(!selectors.hasProcessing());
                selectors.reset();
            }
            else
            {
                inputRow.reset(!selectors.hasProcessing());
            }
        }
        else
        {
            inputRow = new Selector.InputRow(columns,
                                             unmask,
                                             selectors.collectWritetimes(),
                                             selectors.collectTTLs());
        }
    }

    /**
     * Builds the <code>ResultSet</code>
     */
    public ResultSet build()
    {
        if (inputRow  != null)
        {
            selectors.addInputRow(inputRow);
            resultSet.addRow(getOutputRow());
            inputRow.reset(!selectors.hasProcessing());
            selectors.reset();
        }

        // For aggregates we need to return a row even it no records have been found
        if (resultSet.isEmpty() && groupMaker != null && groupMaker.returnAtLeastOneRow())
            resultSet.addRow(getOutputRow());
        return resultSet;
    }

    private List<ByteBuffer> getOutputRow()
    {
        List<ByteBuffer> row = selectors.getOutputRow();
        addSize(row);
        return row;
    }
}
