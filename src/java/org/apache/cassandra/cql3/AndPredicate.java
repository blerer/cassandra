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

import java.util.Objects;

/**
 * A predicate corresponding to an {@code AND} keyword in a {@code WHERE} clause.
 */
public final class AndPredicate extends CqlPredicate
{
    /**
     * The predicate on the left side of the {@code AND} keyword.
     */
    public final CqlPredicate left;

    /**
     * The predicate on the right side of the {@code AND} keyword.
     */
    public final CqlPredicate right;

    /**
     * Creates a new {@code AndPredicate}.
     *
     * @param the predicate on the left side of the {@code AND} keyword.
     * @param the predicate on the right side of the {@code AND} keyword.
     */
    public AndPredicate(CqlPredicate left, CqlPredicate right)
    {
        this.left = left;
        this.right = right;
    }

    @Override
    public boolean containsCustomExpressions()
    {
        return left.containsCustomExpressions() || right.containsCustomExpressions();
    }

    @Override
    public CqlPredicate renameIdentifier(ColumnIdentifier from, ColumnIdentifier to)
    {
        CqlPredicate newLeft = left.renameIdentifier(from, to);
        CqlPredicate newRight = right.renameIdentifier(from, to);
        return left == newLeft && right == newRight ? this : new AndPredicate(newLeft, newRight);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(left, right);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof AndPredicate))
            return false;

        AndPredicate p = (AndPredicate) o;
        return left.equals(p.left) && right.equals(p.right);
    }

    @Override
    public StringBuilder appendCQL(StringBuilder builder)
    {
        left.appendCQL(builder);
        builder.append(" AND ");
        right.appendCQL(builder);
        return builder;
    }
}
