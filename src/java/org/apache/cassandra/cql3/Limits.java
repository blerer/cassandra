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

import java.nio.ByteBuffer;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;
import static org.apache.cassandra.utils.ByteBufferUtil.UNSET_BYTE_BUFFER;

/**
 * The query limits defined by the user (LIMIT and PER PARTITION LIMIT)
 */
public final class Limits
{
    /**
     * A {@code Limits} instance representing no limits.
     */
    public static final Limits NO_LIMIT = new Limits(null, null);

    /**
     * The {@code LIMIT} term or {@code null} if no LIMIT was specified.
     */
    @Nullable
    private final Term limit;

    /**
     * The {@code PER PARTITION LIMIT} term or {@code null} if no ER PARTITION LIMIT was specified.
     */
    @Nullable
    private final Term perPartitionLimit;

    public Limits(@Nullable Term limit,@Nullable Term perPartitionLimit)
    {
        this.limit = limit;
        this.perPartitionLimit = perPartitionLimit;
    }

    public void addFunctionsTo(List<Function> functions)
    {
        if (limit != null)
            limit.addFunctionsTo(functions);

        if (perPartitionLimit != null)
            perPartitionLimit.addFunctionsTo(functions);
    }

    /**
     * Collects the column specification for the bind variables in those {@code Limits}.
     * This is obviously a no-op if the term is Terminal.
     *
     * @param boundNames the variables specification where to collect the
     * bind variables of those {@code Limits} in.
     */
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        if (limit != null)
            limit.collectMarkerSpecification(boundNames);

        if (perPartitionLimit != null)
            perPartitionLimit.collectMarkerSpecification(boundNames);
    }

    /**
     * Returns the limit specified by the user.
     *
     * @return the limit specified by the user or <code>DataLimits.NO_LIMIT</code> if no value
     * as been specified.
     */
    public int limit(QueryOptions options)
    {
        return bindAndGet("LIMIT", limit, options);
    }

    /**
     * Returns the per partition limit specified by the user.
     *
     * @return the per partition limit specified by the user or <code>DataLimits.NO_LIMIT</code> if no value
     * as been specified.
     */
    public int perPartitionLimit(QueryOptions options)
    {
        return bindAndGet("PER PARTITION LIMIT", perPartitionLimit, options);
    }

    private int bindAndGet(String cqlName, Term limit, QueryOptions options)
    {
        if (limit == null)
            return DataLimits.NO_LIMIT;

        ByteBuffer b = checkNotNull(limit.bindAndGet(options), "Invalid null value of %s", cqlName);
        // treat UNSET limit value as 'unlimited'
        if (b == UNSET_BYTE_BUFFER)
            return DataLimits.NO_LIMIT;

        try
        {
            Int32Type.instance.validate(b);
            int userLimit = Int32Type.instance.compose(b);
            checkTrue(userLimit > 0, "%s must be strictly positive", cqlName);
            return userLimit;
        }
        catch (MarshalException e)
        {
            throw invalidRequest("Invalid %s value", cqlName);
        }
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();

        if (limit != null)
        {
            builder.append("LIMIT ")
                   .append(limit);
        }

        if (perPartitionLimit != null)
        {
            if (builder.length() == 0)
                builder.append(' ');

            builder.append("PER PARTITION LIMIT ")
                   .append(perPartitionLimit);
        }

        return builder.toString();
    }

    public static class Raw
    {
        /**
         * A {@code Raw} instance representing no limits.
         */
        public static final Raw NO_LIMIT = new Limits.Raw(null, null);

        public final Term.Raw limit;

        public final Term.Raw perPartitionLimit;

        public Raw(@Nullable Term.Raw limit, @Nullable Term.Raw perPartitionLimit)
        {
            this.limit = limit;
            this.perPartitionLimit = perPartitionLimit;
        }

        public boolean hasLimit()
        {
            return limit != null;
        }

        public boolean hasPerPartitionLimit()
        {
            return perPartitionLimit != null;
        }

        public Limits prepare(TableMetadata table)
        {
            if (limit == null && perPartitionLimit == null)
                return Limits.NO_LIMIT;

            return new Limits(prepare(limit, receiver(table, "limit")),
                              prepare(perPartitionLimit, receiver(table, "per_partition_limit")));
        }

        private static ColumnSpecification receiver(TableMetadata table, String name)
        {
            return new ColumnSpecification(table.keyspace, table.name, new ColumnIdentifier('[' + name + ']', true), Int32Type.instance);
        }

        /** Returns a Term for the limit or null if no limit is set */
        private static Term prepare(Term.Raw limit, ColumnSpecification receiver)
        {
            return limit == null ? null : limit.prepare(receiver.ksName, receiver);
        }
    }
}
