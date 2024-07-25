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
import java.util.List;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.exceptions.ReadSizeAbortException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.FBUtilities;

public class ReadThresholdsListener implements ResultSetBuilder.Listener
{
    private final Logger logger;

    private final ReadQuery readQuery;

    private final ConsistencyLevel cl;

    private final long readSizeWarnThreshold;

    private final long readSizeFailThreshold;

    /**
     * The ResultSet size in bytes
     */
    private long sizeInBytes;

    public ReadThresholdsListener(Logger logger,
                                  ReadQuery readQuery,
                                  ConsistencyLevel cl,
                                  long readSizeWarnThreshold,
                                  long readSizeFailThreshold)
    {
        this.logger = logger;
        this.readQuery = readQuery;
        this.cl = cl;
        this.readSizeWarnThreshold = readSizeWarnThreshold;
        this.readSizeFailThreshold = readSizeFailThreshold;
    }

    @Override
    public void onRowAdded(List<ByteBuffer> row)
    {
        sizeInBytes += measureRow(row);
        // reads aren't failed as soon the size exceeds the failure threshold, they're failed once the failure
        // threshold has been exceeded and we start adding more data. We're slightly more permissive to avoid
        // cases where a row can never be read. Since we only warn/fail after entire rows are read, this will
        // still allow the entire dataset to be read with LIMIT 1 queries, even if every row is oversized
        maybeFail();
    }

    @Override
    public void onResultSetBuilt()
    {
        maybeWarn();
    }

    private long measureRow(List<ByteBuffer> row)
    {
        long size = 0;
        for (int i = 0, m = row.size(); i < m; i++)
        {
            ByteBuffer value = row.get(i);
            size += value != null ? value.remaining() : 0;
        }
        return size;
    }

    private void maybeWarn()
    {
        ColumnFamilyStore store = cfs();
        if (store != null)
            store.metric.coordinatorReadSize.update(sizeInBytes);

        if (readSizeFailThreshold != -1 && sizeInBytes > readSizeWarnThreshold)
        {
            String msg = String.format("Read on table %s has exceeded the size warning threshold of %,d bytes", readQuery.metadata(), readSizeWarnThreshold);
            ClientWarn.instance.warn(msg + " with " + readQuery.loggableTokens());
            logger.warn("{} with query {}", msg, readQuery.toCQLString());
            if (store != null)
                store.metric.coordinatorReadSizeWarnings.mark();
        }
    }

    private void maybeFail()
    {
        if (readSizeFailThreshold != -1 && sizeInBytes > readSizeFailThreshold)
        {
            String msg = String.format("Read on table %s has exceeded the size failure threshold of %,d bytes", readQuery.metadata(), readSizeFailThreshold);
            String clientMsg = msg + " with " + readQuery.loggableTokens();
            ClientWarn.instance.warn(clientMsg);
            logger.warn("{} with query {}", msg, readQuery.toCQLString());
            ColumnFamilyStore store = cfs();
            if (store != null)
            {
                store.metric.coordinatorReadSizeAborts.mark();
                store.metric.coordinatorReadSize.update(sizeInBytes);
            }
            // read errors require blockFor and recieved (it is in the protocol message), but this isn't known;
            // to work around this, treat the coordinator as the only response we care about and mark it failed
            ReadSizeAbortException exception = new ReadSizeAbortException(clientMsg, cl, 0, 1, true,
                                                                          ImmutableMap.of(FBUtilities.getBroadcastAddressAndPort(), RequestFailureReason.READ_SIZE));
            StorageProxy.recordReadRegularAbort(cl, exception);
            throw exception;
        }
    }

    private ColumnFamilyStore cfs()
    {
        return Schema.instance.getColumnFamilyStoreInstance(readQuery.metadata().id);
    }
}
