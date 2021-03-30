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

/**
 * A predicate that is part of a WHERE clause.
 */
public abstract class CqlPredicate
{
    /**
     * Returns a CQL representation of this predicate.
     *
     * @return a CQL representation of this predicate
     */
    public final String toCQLString()
    {
        return appendCQL(new StringBuilder()).toString();
    }

    @Override
    public final String toString()
    {
        return toCQLString();
    }

    /**
     * Appends the CQL representation of this predicate to the builder.
     *
     * @return the builder 
     */
    public abstract StringBuilder appendCQL(StringBuilder builder);

    /**
     * Checks if this predicate contains some custom expressions.
     *
     * @return {@code true} if this predicate contains some custom expressions, {@code false} otherwise.
     */
    public abstract boolean containsCustomExpressions();

    /**
     * Renames an identifier in this Relation, if applicable.
     *
     * @param from the old identifier
     * @param to the new identifier
     * @return this object, if the old identifier is not in the set of entities that this predicate covers; otherwise
     *         a new Relation with "from" replaced by "to" is returned.
     */
    public abstract CqlPredicate renameIdentifier(ColumnIdentifier from, ColumnIdentifier to);
}
