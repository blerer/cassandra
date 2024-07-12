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

import javax.annotation.Nullable;

import org.apache.cassandra.cql3.SortSpecification.Kind;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

/**
 * A single element of an ORDER BY clause.
 * <code>ORDER BY ordering1 [, ordering2 [, ...]] </code>
 * <p>
 * An ordering comprises an expression that produces the values to compare against each other
 * and a sorting direction (ASC, DESC).
 */
public final class SortExpression
{
    public enum Direction
    {
        ASC,
        DESC;

        public boolean isAscending()
        {
            return this == ASC;
        }
    }

    private final Kind kind;

    private final ColumnMetadata column;

    private final Term vector;

    private final Direction direction;

    public SortExpression(Kind kind, ColumnMetadata column, @Nullable  Term vector, Direction direction)
    {
        assert direction != null;
        assert kind != Kind.ANN || vector != null : "vector should not be null for ANN ordering";

        if (kind == Kind.ANN)
        {
            AbstractType<?> type = column.type;
            checkTrue(type.isVector() && !(((VectorType<?>) type).elementType instanceof FloatType),
                      "ANN ordering is only supported on float vector indexes");
            checkTrue(direction.isAscending(), "Descending ANN ordering is not supported");
        }

        this.kind = kind;
        this.column = column;
        this.vector = vector;
        this.direction = direction;
    }

    public Kind kind()
    {
        return kind;
    }

    public ColumnMetadata column()
    {
        return column;
    }

    public Direction direction()
    {
        return direction;
    }

    public Term vector()
    {
        return vector;
    }

    /**
     * Collects the column specification for the bind variables in this OrderingExpression.
     *
     * @param boundNames the variables specification where to collect the
     * bind variables of this expression in.
     */
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        if (vector != null)
            vector.collectMarkerSpecification(boundNames);
    }

    /**
     * Represents ANTLR's abstract syntax tree of a single element in the {@code ORDER BY} clause.
     * This comes directly out of CQL parser.
     */
    public static class Raw
    {
        private final Kind kind;

        private final ColumnIdentifier identifier;

        private final Term.Raw rawVector;

        private final Direction direction;

        private Raw(Kind kind, ColumnIdentifier identifier, @Nullable Term.Raw rawVector, Direction direction)
        {
            assert kind != Kind.ANN || rawVector != null : "rawVector should not be null for ANN ordering";
            this.kind = kind;
            this.identifier = identifier;
            this.rawVector = rawVector;
            this.direction = direction;
        }

        public Kind kind()
        {
            return kind;
        }

        /**
         * Bind this {@code Raw} instance to the schema and return the resulting {@code OrderingExpression}.
         *
         * @param table the table schema
         * @return the {@code OrderingExpression} resulting from the schema binding
         */
        public SortExpression prepare(TableMetadata table)
        {
            ColumnMetadata column = table.getExistingColumn(identifier);
            Term vector = rawVector == null ? null : rawVector.prepare(table.keyspace, column);
            return new SortExpression(kind, column, vector, direction);
        }
    }
}



