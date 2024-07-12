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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.cql3.restrictions.SimpleRestriction;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.cql3.terms.Terms;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;

public abstract class SortSpecification
{
    public enum Kind
    {
        CLUSTERING,
        ANN;
    }

    private final Kind kind;

    public SortSpecification(Kind kind)
    {
        this.kind = kind;
    }

    /**
     * Collects the column specification for the bind variables in this OrderingExpression.
     *
     * @param boundNames the variables specification where to collect the
     * bind variables of this expression in.
     */
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
    }

    public SimpleRestriction restriction()
    {
        throw new UnsupportedOperationException();
    }

    public boolean isReversedClusteringOrder()
    {
        return false;
    }

    public boolean ignoreLimit()
    {
        return false;
    }

    public abstract boolean requirePostQueryOrdering();

    public abstract Comparator<List<ByteBuffer>> postQueryOrderingComparator(Index index, QueryOptions options);

    private static class ClusteringSortSpecification extends SortSpecification
    {
        private final boolean isReversed;

        private final Comparator<List<ByteBuffer>> postQueryOrderingComparator;

        public ClusteringSortSpecification(boolean isReversed, Comparator<List<ByteBuffer>> postQueryOrderingComparator)
        {
            super(Kind.CLUSTERING);
            this.isReversed = isReversed;
            this.postQueryOrderingComparator = postQueryOrderingComparator;
        }

        @Override
        public boolean isReversedClusteringOrder()
        {
            return isReversed;
        }

        public boolean ignoreLimit()
        {
            return requirePostQueryOrdering();
        }

        @Override
        public boolean requirePostQueryOrdering()
        {
            return postQueryOrderingComparator != null;
        }

        @Override
        public Comparator<List<ByteBuffer>> postQueryOrderingComparator(Index index, QueryOptions options)
        {
            if (!requirePostQueryOrdering())
                throw new IllegalStateException("Post query ordering is not needed.");

            return postQueryOrderingComparator;
        }
    }

    private static final class AnnSortSpecification extends SortSpecification
    {
        private final ColumnMetadata column;

        private final int columnIndex;

        private final Term vector;

        public AnnSortSpecification(ColumnMetadata column, int columnIndex, Term vector)
        {
            super(Kind.ANN);
            this.column = column;
            this.columnIndex = columnIndex;
            this.vector = vector;
        }

        public SimpleRestriction restriction()
        {
            return new SimpleRestriction(ColumnsExpression.singleColumn(column),
                                         Operator.ANN,
                                         Terms.of(vector));
        }

        @Override
        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
            this.vector.collectMarkerSpecification(boundNames);
        }

        @Override
        public boolean requirePostQueryOrdering()
        {
            return true;
        }

        @Override
        public Comparator<List<ByteBuffer>> postQueryOrderingComparator(Index index, QueryOptions options)
        {
            Comparator<ByteBuffer> comparator = index.getPostQueryOrdering(restriction(), options);
            return PostQueryOrderingComparators.singleColumnComparator(columnIndex, comparator);
        }
    }

    public static class Raw
    {
        private Kind kind;
        private final List<SortExpression.Raw> rawExpressions;

        public Raw(List<SortExpression.Raw> rawExpressions)
        {
            this.kind = kind(rawExpressions);
            this.rawExpressions = rawExpressions;
        }

        private Kind kind(List<SortExpression.Raw> rawExpressions)
        {
            int annCount = 0;
            for (SortExpression.Raw raw : rawExpressions)
            {
                if (raw.kind() == Kind.ANN)
                    annCount++;
            }

            checkFalse(annCount > 1, "Cannot specify more than one ANN ordering");
            checkTrue(annCount != 1 || rawExpressions.size() == 1, "Cannot specify more than one ANN ordering");

            return annCount == 1 ? Kind.ANN : Kind.CLUSTERING;
        }

        public SortSpecification prepare(TableMetadata table, Selection selection, StatementRestrictions restrictions)
        {
            List<SortExpression> expressions = new ArrayList<>(rawExpressions.size());
            for (SortExpression.Raw raw : rawExpressions)
            {
                expressions.add(raw.prepare(table));
            }

            if (kind == Kind.ANN)
            {
                SortExpression expression = expressions.get(0);
                ColumnMetadata column = expression.column();
                return new AnnSortSpecification(column, selection.getOrderingIndex(column), expression.vector());
            }

            checkFalse(restrictions.usesSecondaryIndexing(), "ORDER BY with 2ndary indexes is not supported, except for ANN queries.");
            checkFalse(restrictions.isKeyRange(), "ORDER BY is only supported when the partition key is restricted by an EQ or an IN.");

            Comparator<List<ByteBuffer>> postQueryOrderingComparator = restrictions.keyIsInRelation() ? preparePostQueryOrderingComparator(expressions, selection)
                                                                                                      : null;

            boolean isReversed = isReversed(table, expressions, restrictions);

            if (isReversed && postQueryOrderingComparator != null)
                postQueryOrderingComparator = postQueryOrderingComparator.reversed();

            return new ClusteringSortSpecification(isReversed, postQueryOrderingComparator);
        }

        private static Comparator<List<ByteBuffer>> preparePostQueryOrderingComparator(List<SortExpression> expressions, Selection selection)
        {
            if (expressions.size() == 1)
            {
                ColumnMetadata column = expressions.get(0).column();
                return PostQueryOrderingComparators.singleColumnComparator(selection.getOrderingIndex(column), column.type);
            }

            List<Integer> indexes = new ArrayList<>(expressions.size());
            List<Comparator<ByteBuffer>> types = new ArrayList<>(expressions.size());

            for (SortExpression expression : expressions)
            {
                ColumnMetadata column = expression.column();
                indexes.add(selection.getOrderingIndex(column));
                types.add(column.type);
            }
            return PostQueryOrderingComparators.compositeColumnComparator(indexes, types);
        }

        private boolean isReversed(TableMetadata table, List<SortExpression> expressions, StatementRestrictions restrictions)
        {
            Boolean[] reversedMap = new Boolean[table.clusteringColumns().size()];
            int i = 0;
            for (SortExpression expression : expressions)
            {
                ColumnMetadata column = expression.column();
                boolean reversed = !expression.direction().isAscending();

                checkTrue(column.isClusteringColumn(),
                          "Order by is currently only supported on the clustered columns of the PRIMARY KEY, got %s", column.name);

                while (i != column.position())
                {
                    checkTrue(restrictions.isColumnRestrictedByEq(table.clusteringColumns().get(i++)),
                              "Order by currently only supports the ordering of columns following their declared order in the PRIMARY KEY");
                }
                i++;
                reversedMap[column.position()] = (reversed != column.isReversedType());
            }

            // Check that all boolean in reversedMap, if set, agrees
            Boolean isReversed = null;
            for (Boolean b : reversedMap)
            {
                // Column on which order is specified can be in any order
                if (b == null)
                    continue;

                if (isReversed == null)
                {
                    isReversed = b;
                    continue;
                }
                checkTrue(isReversed.equals(b), "Unsupported order by relation");
            }
            assert isReversed != null;
            return isReversed;
        }
    }
}
