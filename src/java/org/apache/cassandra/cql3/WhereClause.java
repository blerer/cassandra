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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.cql3.restrictions.CustomIndexExpression;

public final class WhereClause
{
    private static final WhereClause EMPTY = new WhereClause(null);

    private final Optional<CqlPredicate> maybePredicates;

    private final boolean containsCustomExpressions;

    public final List<ColumnsPredicate> relations;
    public final List<CustomIndexExpression> expressions;

    public WhereClause(CqlPredicate predicates)
    {
        System.out.println(predicates);
        this.maybePredicates = Optional.ofNullable(predicates);
        this.containsCustomExpressions = maybePredicates.isPresent() ? predicates.containsCustomExpressions() : false;
        this.relations = new ArrayList<>();
        this.expressions = new ArrayList<>();
        if (maybePredicates.isPresent())
        {
            addPredicat(maybePredicates.get(), relations, expressions);
        }
    }

    public void addPredicat(CqlPredicate predicate, List<ColumnsPredicate> relations, List<CustomIndexExpression> expessions)
    {
        if (predicate instanceof AndPredicate)
        {
            AndPredicate and = (AndPredicate) predicate;
            addPredicat(and.left, relations, expessions);
            addPredicat(and.right, relations, expessions);
        }
        else if (predicate instanceof ColumnsPredicate)
        {
            relations.add((ColumnsPredicate) predicate);
        }
        else
        {
            expessions.add((CustomIndexExpression) predicate);
        }
    }

    public static WhereClause empty()
    {
        return EMPTY;
    }

    public boolean containsCustomExpressions()
    {
        return containsCustomExpressions;
    }

    /**
     * Renames identifiers in all relations
     * @param from the old identifier
     * @param to the new identifier
     * @return a new WhereClause with with "from" replaced by "to" in all relations
     */
    public WhereClause renameIdentifier(ColumnIdentifier from, ColumnIdentifier to)
    {
        if (!maybePredicates.isPresent())
            return this;

        CqlPredicate predicates = maybePredicates.get();
        CqlPredicate newPredicates = predicates.renameIdentifier(from, to);
        return newPredicates == predicates ? this : new WhereClause(newPredicates);
    }

    public static WhereClause parse(String cql) throws RecognitionException
    {
        return CQLFragmentParser.parseAnyUnhandled(CqlParser::whereClause, cql);
    }

    @Override
    public String toString()
    {
        return toCQLString();
    }

    /**
     * Returns a CQL representation of this WHERE clause.
     *
     * @return a CQL representation of this WHERE clause
     */
    public String toCQLString()
    {
        return maybePredicates.isPresent() ? maybePredicates.get().toCQLString() : "";
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof WhereClause))
            return false;

        WhereClause wc = (WhereClause) o;
        return maybePredicates.equals(wc.maybePredicates);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(maybePredicates);
    }

    public static final class Builder
    {
        private CqlPredicate predicates;

        public Builder add(CqlPredicate predicate)
        {
            predicates = predicates == null ? predicate : new AndPredicate(predicates, predicate);
            return this;
        }

        public WhereClause build()
        {
            return new WhereClause(predicates);
        }
    }
}
