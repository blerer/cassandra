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

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.pager.AggregationQueryPager;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.Dispatcher;

/**
 * A wrapper above a {@code QueryPager} used to simplify the logic in {@code SelectStatement}.
 */
public final class Pager
{
    /**
     * The {@code QueryPager} or {@code null} if this pager will return a single page.
     */
    private final QueryPager queryPager;

    /**
     * The function used to fetch the data.
     */
    private final PageFetcher pageFetcher;

    private final boolean isUserPagingEnabled;

    private Pager(QueryPager queryPager, PageFetcher pageFetcher, boolean isUserPagingEnabled)
    {
        this.queryPager = queryPager;
        this.pageFetcher = pageFetcher;
        this.isUserPagingEnabled = isUserPagingEnabled;
    }

    /**
     * Returns a pager for internal queries.
     *
     * @param query the ReadQuery
     * @param options the query options
     * @param state the client state
     * @param forAggregation {@code true} if the query perform some aggregations, {@code false} otherwise.
     * @param executionController the execution controller for internal queries
     * @return a pager for internal queries.
     */
    public static Pager forInternalQuery(ReadQuery query, QueryOptions options, ClientState state, boolean forAggregation, ReadExecutionController executionController)
    {
        int pageSize = options.getPageSize();
        boolean isUserPagingEnabled = pageSize > 0;

        if (shouldPagingBeDisabled(query, forAggregation, pageSize))
            return new Pager(null, t -> query.executeInternal(executionController), isUserPagingEnabled);

        Guardrails.pageSize.guard(pageSize, query.metadata().name, false, state);

        QueryPager queryPager = queryPager(query, options, forAggregation);

        return new Pager(queryPager, t -> queryPager.fetchPageInternal(pageSize, executionController), isUserPagingEnabled);
    }

    /**
     * Returns a pager for distributed queries.
     *
     * @param query the ReadQuery
     * @param options the query options
     * @param state the client state
     * @param forAggregation {@code true} if the query perform some aggregations, {@code false} otherwise.
     * @return a pager for distributed queries.
     */
    public static Pager forDistributedQuery(ReadQuery query, QueryOptions options, ClientState state, boolean forAggregation)
    {
        int pageSize = options.getPageSize();
        ConsistencyLevel cl = options.getConsistency();
        boolean isUserPagingEnabled = pageSize > 0;

        if (shouldPagingBeDisabled(query, forAggregation, pageSize))
            return new Pager(null, t -> query.execute(cl, state, t), isUserPagingEnabled);

        Guardrails.pageSize.guard(pageSize, query.metadata().name, false, state);

        QueryPager queryPager = queryPager(query, options, forAggregation);

        return new Pager(queryPager, t -> queryPager.fetchPage(pageSize, cl, state, t), isUserPagingEnabled);
    }

    /**
     * Fetch the next page of data.
     *
     * @param requestTime the request time
     * @return an iterator over the next page of data.
     */
    public PartitionIterator fetchPage(Dispatcher.RequestTime requestTime)
    {
        return pageFetcher.fetch(requestTime);
    }

    /**
     * Checks if the user requested paging.
     * @return {@code true} if the user requested paging, {@code false} otherwise.
     */
    public boolean isUserPagingEnabled()
    {
        return isUserPagingEnabled;
    }

    /**
     * Checks if this pager has exhausted all the results (e.g. if all the page have been returned).
     * @return {@code true} if this pager has exhausted all the results, {@code false} otherwise.
     */
    public boolean isExhausted()
    {
        return queryPager == null || queryPager.isExhausted();
    }

    public PagingState state()
    {
        return queryPager == null ? null : queryPager.state();
    }

    /**
     * Checks if paging should be disabled.
     * <p>Paging should be disabled if the query results are not aggregated and :
     *   <ul>
     *       <li>paging is disabled (pageSize <= 0)</li>
     *       <li>limit is smaller than the page size</li>
     *       <li>it is a top K query</li>
     *   </ul>
     * </p>
     * @param query the ReadQuery
     * @param forAggregation {@code true} if the query perform some aggregations, {@code false} otherwise.
     * @param pageSize the page size
     * @return {@code true} if paging should be disabled, {@code false} otherwise.
     */
    private static boolean shouldPagingBeDisabled(ReadQuery query, boolean forAggregation, int pageSize)
    {
        return !forAggregation && (pageSize <= 0 || (query.limits().count() <= pageSize) || query.isTopK());
    }

    /**
     * Creates the {@code QueryPager} used under the hood to page the data if paging is enabled.
     * @param query the ReadQuery
     * @param options the query options
     * @param forAggregation {@code true} if the query perform some aggregations, {@code false} otherwise.
     * @return the {@code QueryPager} used under the hood to page the data if paging is enabled.
     */
    private static QueryPager queryPager(ReadQuery query, QueryOptions options, boolean forAggregation)
    {
        QueryPager pager = query.getPager(options.getPagingState(), options.getProtocolVersion());

        if (!forAggregation || query.isEmpty())
            return pager;

        return new AggregationQueryPager(pager, query.limits());
    }

    /**
     * Function used to fetch the data.
     */
    @FunctionalInterface
    private interface PageFetcher
    {
        PartitionIterator fetch(Dispatcher.RequestTime requestTime);
    }
}
