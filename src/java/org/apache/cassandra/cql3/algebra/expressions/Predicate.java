/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.algebra.expressions;

import java.util.List;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.algebra.RowExpression;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * Predicate used to filter rows.
 */
public abstract class Predicate implements RowExpression
{
    /**
     * The equality or logical operator used by this predicate.
     */
    protected final Operator operator;

    /**
     * Represents a field value or a set of fields values. Those values can be fixed or bound at execution time.
     * This value is null when the operators are {@code IS NULL} or {@code IS NOT NULL}.
     */
    protected final Term term;

    public Predicate(Operator operator, Term term)
    {
        this.operator = operator;
        this.term = term;
    }

    /**
     * Returns the equality or logical operator used by this predicate
     *
     * @return the equality or logical operator used by this predicate
     */
    public Operator operator()
    {
        return operator;
    }

    /**
     * Returns the metadata of the first column.
     *
     * @return the metadata of the first column
     */
    public abstract ColumnMetadata firstColumn();

    /**
     * Returns the metadata of the last column.
     *
     * @return the metadata of the last column
     */
    public abstract ColumnMetadata lastColumn();

    /**
     * Returns the column metadata in position order.
     *
     * @return the column metadata in position order
     */
    public abstract List<ColumnMetadata> columns();

    /**
     * Adds all functions (native and user-defined) used by this predicate to the specified list.
     *
     * @param functions the list to add to
     */
    public void addFunctionsTo(List<Function> functions)
    {
        term.addFunctionsTo(functions);
    }
}
