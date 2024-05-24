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
package org.apache.cassandra.cql3.conditions;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.terms.Lists;
import org.apache.cassandra.cql3.terms.Maps;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.cql3.terms.Terms;
import org.apache.cassandra.cql3.terms.UserTypes;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import static org.apache.cassandra.cql3.statements.RequestValidations.*;

/**
 * A CQL3 condition on the value of a column or collection element.  For example, "UPDATE .. IF a = 0".
 */
public final class ColumnCondition
{
    /**
     * The columns expression to which the condition applies.
     */
    public final ColumnsExpression columnsExpression;

    /**
     * The operator
     */
    public final Operator operator;

    /**
     * The values
     */
    private final Terms values;

    public ColumnCondition(ColumnsExpression columnsExpression, Operator operator, Terms values)
    {
        this.columnsExpression = columnsExpression;
        this.operator = operator;
        this.values = values;
    }

    /**
     * Adds functions for the bind variables of this operation.
     *
     * @param functions the list of functions to get add
     */
    public void addFunctionsTo(List<Function> functions)
    {
        columnsExpression.addFunctionsTo(functions);
        values.addFunctionsTo(functions);
    }

    /**
     * Collects the column specification for the bind variables of this operation.
     *
     * @param boundNames the list of column specification where to collect the
     * bind variables of this term in.
     */
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        columnsExpression.collectMarkerSpecification(boundNames);
        values.collectMarkerSpecification(boundNames);
    }

    public ColumnCondition.Bound bind(QueryOptions options)
    {
        switch (columnsExpression.kind())
        {
            case SINGLE_COLUMN:
                return bindSingleColumn(options);
            case ELEMENT:
                return bindElement(options);
            default:
                throw new UnsupportedOperationException();
        }
    }

    private Bound bindSingleColumn(QueryOptions options)
    {
        ColumnMetadata column = columnsExpression.firstColumn();
        if (column.type.isMultiCell())
            return new MultiCellBound(column, operator, toValue(column.type, bindAndGetTerms(options)));

        return new SimpleBound(column, operator, toValue(column.type, bindAndGetTerms(options)));
    }

    private ColumnCondition.Bound bindElement(QueryOptions options)
    {
        ColumnMetadata column = columnsExpression.firstColumn();
        ByteBuffer keyOrIndex = columnsExpression.element().bindAndGet(options);
        if (column.type.isCollection())
        {
            checkNotNull(keyOrIndex, "Invalid null value for %s element access", column.type instanceof MapType ? "map" : "list");
        }
        return new ElementOrFieldAccessBound(column, keyOrIndex, operator, toValue(columnsExpression.type(), bindAndGetTerms(options)));
    }

    private ByteBuffer toValue(AbstractType<?> type, List<ByteBuffer> values)
    {
        if (operator.isIN())
            return ListType.getInstance(type, false).pack(values);

        ByteBuffer value = values.get(0);
        if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
            throw invalidRequest("Invalid 'unset' value in condition");

        return value;
    }

    private List<ByteBuffer> bindAndGetTerms(QueryOptions options)
    {
        List<ByteBuffer> buffers = values.bindAndGet(options);
        checkFalse(buffers == null && operator.isIN(), "Invalid null list in IN condition");
        checkFalse(buffers == Term.UNSET_LIST, "Invalid 'unset' value in condition");
        return filterUnsetValuesIfNeeded(buffers, ByteBufferUtil.UNSET_BYTE_BUFFER);
    }

    private <T> List<T> filterUnsetValuesIfNeeded(List<T> values, T unsetValue)
    {
        if (!operator.isIN())
            return values;

        List<T> filtered = new ArrayList<>(values.size());
        for (int i = 0, m = values.size(); i < m; i++)
        {
            T value = values.get(i);
            if (value != unsetValue)
                filtered.add(value);
        }
        return filtered;
    }

    public static abstract class Bound
    {
        protected final ColumnMetadata column;
        protected final Operator operator;
        protected final ByteBuffer value;

        protected Bound(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            this.column = column;
            this.operator = operator;
            this.value = value;
        }

        /**
         * Validates whether this condition applies to {@code current}.
         */
        public abstract boolean appliesTo(Row row);
    }

    /**
     * A condition on a single non-collection column.
     */
    private static final class SimpleBound extends Bound
    {
        private SimpleBound(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            super(column, operator, value);
        }

        @Override
        public boolean appliesTo(Row row)
        {
            return operator.isSatisfiedBy(column.type, rowValue(row), value);
        }

        private ByteBuffer rowValue(Row row)
        {
            // If we're asking for a given cell, and we didn't got any row from our read, it's
            // the same as not having said cell.
            if (row == null)
                return null;

            Cell<?> c = row.getCell(column);
            return c == null ? null : c.buffer();
        }
    }

    /**
     * A condition on an element of a collection column.
     */
    private static final class ElementOrFieldAccessBound extends Bound
    {
        private final AbstractType<?> elementType;

        /**
         * The collection element
         */
        private final ByteBuffer keyOrIndex;


        private ElementOrFieldAccessBound(ColumnMetadata column,
                                          ByteBuffer keyOrIndex,
                                          Operator operator,
                                          ByteBuffer value)
        {
            super(column, operator, value);
            this.elementType = ((MultiElementType<?>) column.type).elementType(keyOrIndex);
            this.keyOrIndex = keyOrIndex;
        }

        @Override
        public boolean appliesTo(Row row)
        {
            ByteBuffer element = ((MultiElementType<?>) column.type).getElement(columnData(row), keyOrIndex);
            return operator.isSatisfiedBy(elementType, element, value);
        }

        public ColumnData columnData(Row row)
        {
            return row == null ? null : row.getColumnData(column);
        }
    }

    /**
     * A condition on a multicell column.
     */
    private static final class MultiCellBound extends Bound
    {
        public MultiCellBound(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            super(column, operator, value);
            assert column.type.isMultiCell();
        }

        public boolean appliesTo(Row row)
        {
            ComplexColumnData columnData = row == null ? null : row.getComplexColumnData(column);
            return operator.isSatisfiedBy((MultiElementType<?>) column.type, columnData, value);
        }
    }

    public static class Raw
    {
        private final ColumnsExpression.Raw rawExpressions;
        private final Terms.Raw values;

        // Can be null, only used with the syntax "IF m[e] = ..." (in which case it's 'e')
        private final Term.Raw collectionElement;

        // Can be null, only used with the syntax "IF udt.field = ..." (in which case it's 'field')
        private final FieldIdentifier udtField;

        private final Operator operator;

        private Raw(ColumnsExpression.Raw columnExpressions, Term.Raw collectionElement, FieldIdentifier udtField, Operator op, Terms.Raw values)
        {
            this.rawExpressions = columnExpressions;
            this.values = values;
            this.collectionElement = collectionElement;
            this.udtField = udtField;
            this.operator = op;
        }

        /**
         * Create condition on a column. For example: "IF col = 'foo'" or "IF col IN ('foo', 'bar', ...)"
         */
        public static Raw simpleCondition(ColumnIdentifier column, Operator op, Terms.Raw values)
        {
            return new Raw(ColumnsExpression.Raw.singleColumn(column), null, null, op, values);
        }

        /**
         * Create a condition on a collection element. For example: "IF col['key'] = 'foo'"
         */
        public static Raw collectionElementCondition(ColumnIdentifier column, Term.Raw collectionElement, Operator op, Terms.Raw values)
        {
            return new Raw(ColumnsExpression.Raw.collectionElement(column, collectionElement), collectionElement, null, op, values);
        }

        /**
         * Create a condition on a UDT field. For example: "IF col.field = 'foo'"
         */
        public static Raw udtFieldCondition(ColumnIdentifier column, FieldIdentifier udtField, Operator op, Terms.Raw values)
        {
            return new Raw(ColumnsExpression.Raw.udtField(column, udtField), null, udtField, op, values);
        }

        public ColumnIdentifier column()
        {
            return rawExpressions.identifiers().get(0);
        }

        public ColumnCondition prepare(TableMetadata table)
        {
            ColumnsExpression expression = rawExpressions.prepare(table);
            ColumnSpecification receiver = receiver(table, expression);
            validateOperationOnDurations(receiver.type);
            return new ColumnCondition(expression, operator, prepareTerms(table.keyspace, receiver));
        }

        private ColumnSpecification receiver(TableMetadata table, ColumnsExpression expression)
        {
            ColumnMetadata receiver = table.getExistingColumn(column());
            checkFalse(receiver.isPrimaryKeyColumn(), "PRIMARY KEY column '%s' cannot have IF conditions", receiver.name);

            if (receiver.type instanceof CounterColumnType)
                throw invalidRequest("Conditions on counters are not supported");

            if (expression.kind() == ColumnsExpression.Kind.ELEMENT)
            {
                switch (expression.elementKind())
                {
                    case COLLECTION_ELEMENT:
                        switch ((((CollectionType<?>) receiver.type).kind))
                        {
                            case LIST:
                                return Lists.valueSpecOf(receiver);
                            case MAP:
                                return Maps.valueSpecOf(receiver);
                            case SET:
                                throw invalidRequest("Invalid element access syntax for set column %s", receiver.name);
                            default:
                                throw new AssertionError();
                        }

                    case UDT_FIELD:
                        int fieldPosition = ((UserType) receiver.type).fieldPosition(udtField);
                        return UserTypes.fieldSpecOf(receiver, fieldPosition);
                }
            }
            return receiver;
        }


        private Terms prepareTerms(String keyspace, ColumnSpecification receiver)
        {
            checkFalse(operator == Operator.CONTAINS_KEY && !(receiver.type instanceof MapType),
                       "Cannot use CONTAINS KEY on non-map column %s", receiver.name);
            checkFalse(operator == Operator.CONTAINS && !(receiver.type.isCollection()),
                       "Cannot use CONTAINS on non-collection column %s", receiver.name);

            if (operator == Operator.CONTAINS || operator == Operator.CONTAINS_KEY)
                receiver = ((CollectionType<?>) receiver.type).makeCollectionReceiver(receiver, operator == Operator.CONTAINS_KEY);

            return values.prepare(keyspace, receiver);
        }

        private void validateOperationOnDurations(AbstractType<?> type)
        {
            if (type.referencesDuration() && operator.isSlice())
            {
                checkFalse(type.isCollection(), "Slice conditions are not supported on collections containing durations");
                checkFalse(type.isTuple(), "Slice conditions are not supported on tuples containing durations");
                checkFalse(type.isUDT(), "Slice conditions are not supported on UDTs containing durations");
                throw invalidRequest("Slice conditions ( %s ) are not supported on durations", operator);
            }
        }

        public boolean containsBindMarkers()
        {
            return values.containsBindMarkers() || (collectionElement != null && collectionElement.containsBindMarkers());
        }

        @VisibleForTesting
        public String toCQLString()
        {
            return String.format("%s %s %s", rawExpressions == null ? null : rawExpressions.toCQLString(), operator, values.getText());
        }

        @Override
        public String toString()
        {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        }
    }
}
