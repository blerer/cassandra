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
public abstract class ColumnCondition
{
    public final ColumnMetadata column;
    public final Operator operator;
    private final Terms terms;

    private ColumnCondition(ColumnMetadata column, Operator op, Terms terms)
    {
        this.column = column;
        this.operator = op;
        this.terms = terms;
    }

    /**
     * Adds functions for the bind variables of this operation.
     *
     * @param functions the list of functions to get add
     */
    public void addFunctionsTo(List<Function> functions)
    {
        terms.addFunctionsTo(functions);
    }

    /**
     * Collects the column specification for the bind variables of this operation.
     *
     * @param boundNames the list of column specification where to collect the
     * bind variables of this term in.
     */
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        terms.collectMarkerSpecification(boundNames);
    }

    public abstract ColumnCondition.Bound bind(QueryOptions options);

    protected final List<ByteBuffer> bindAndGetTerms(QueryOptions options)
    {
        List<ByteBuffer> buffers = terms.bindAndGet(options);
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

    /**
     * Simple condition (e.g. <pre>IF v = 1</pre>).
     */
    private static final class SimpleColumnCondition extends ColumnCondition
    {
        public SimpleColumnCondition(ColumnMetadata column, Operator op, Terms values)
        {
            super(column, op, values);
        }

        public Bound bind(QueryOptions options)
        {
            if (column.type.isMultiCell())
                return new MultiCellBound(column, operator, bindAndGetTerms(options));

            return new SimpleBound(column, operator, bindAndGetTerms(options));
        }
    }

    /**
     * A condition on a collection element (e.g. <pre>IF l[1] = 1</pre>).
     */
    private static class CollectionElementCondition extends ColumnCondition
    {
        private final Term collectionElement;

        public CollectionElementCondition(ColumnMetadata column, Term collectionElement, Operator op, Terms values)
        {
            super(column, op, values);
            this.collectionElement = collectionElement;
        }

        public void addFunctionsTo(List<Function> functions)
        {
            collectionElement.addFunctionsTo(functions);
            super.addFunctionsTo(functions);
        }

        public void collectMarkerSpecification(VariableSpecifications boundNames)
        {
            collectionElement.collectMarkerSpecification(boundNames);
            super.collectMarkerSpecification(boundNames);
        }

        public Bound bind(QueryOptions options)
        {
            return new ElementOrFieldAccessBound(column.type instanceof MapType ? Accessor.forMapElement(column, collectionElement.bindAndGet(options))
                                                                                : Accessor.forListElement(column, collectionElement.bindAndGet(options)),
                                                 operator, bindAndGetTerms(options));
        }
    }

    /**
     *  A condition on a UDT field (e.g. <pre>IF v.a = 1</pre>).
     */
    private final static class UDTFieldCondition extends ColumnCondition
    {
        private final FieldIdentifier udtField;

        public UDTFieldCondition(ColumnMetadata column, FieldIdentifier udtField, Operator op, Terms values)
        {
            super(column, op, values);
            assert udtField != null;
            this.udtField = udtField;
        }

        public Bound bind(QueryOptions options)
        {
            return new ElementOrFieldAccessBound(Accessor.forUdtField(column, udtField), operator, bindAndGetTerms(options));
        }
    }

    /**
     *  A regular column, simple condition.
     */
    public static ColumnCondition condition(ColumnMetadata column, Operator op, Terms terms)
    {
        return new SimpleColumnCondition(column, op, terms);
    }

    /**
     * A collection column, simple condition.
     */
    public static ColumnCondition condition(ColumnMetadata column, Term collectionElement, Operator op, Terms terms)
    {
        return new CollectionElementCondition(column, collectionElement, op, terms);
    }

    /**
     * A UDT column, simple condition.
     */
    public static ColumnCondition condition(ColumnMetadata column, FieldIdentifier udtField, Operator op, Terms terms)
    {
        return new UDTFieldCondition(column, udtField, op, terms);
    }

    public static abstract class Bound
    {
        public final ColumnMetadata column;
        public final Operator comparisonOperator;

        protected Bound(ColumnMetadata column, Operator operator)
        {
            this.column = column;
            // If the operator is an IN we want to compare the value using an EQ.
            this.comparisonOperator = operator;
        }

        /**
         * Validates whether this condition applies to {@code current}.
         */
        public abstract boolean appliesTo(Row row);

        public ByteBuffer getCollectionElementValue()
        {
            return null;
        }
    }

    /**
     * A condition on a single non-collection column.
     */
    private static final class SimpleBound extends Bound
    {
        /**
         * The condition values
         */
        private final ByteBuffer value;

        private SimpleBound(ColumnMetadata column, Operator operator, List<ByteBuffer> values)
        {
            super(column, operator);
            this.value = operator.isIN() ? ListType.getInstance(column.type, false).pack(values) : values.get(0);
            if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
                throw invalidRequest("Invalid 'unset' value in condition");
        }

        @Override
        public boolean appliesTo(Row row)
        {
            return comparisonOperator.isSatisfiedBy(column.type, rowValue(row), value);
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
        /**
         * The collection element
         */
        private final Accessor elementAccessor;

        /**
         * The conditions values.
         */
        private final ByteBuffer value;

        private ElementOrFieldAccessBound(Accessor elementAccessor,
                                          Operator operator,
                                          List<ByteBuffer> values)
        {
            super(null, operator);
            this.elementAccessor = elementAccessor;
            this.value = operator.isIN() ? ListType.getInstance(elementAccessor.type(), false).pack(values)
                                         : values.get(0);
            if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
                throw invalidRequest("Invalid 'unset' value in condition");
        }

        @Override
        public boolean appliesTo(Row row)
        {
            return comparisonOperator.isSatisfiedBy(elementAccessor.type(), elementAccessor.elementValue(row), value);
        }
    }

    /**
     * A condition on a multicell column.
     */
    private static final class MultiCellBound extends Bound
    {
        private final ByteBuffer value;

        public MultiCellBound(ColumnMetadata column, Operator operator, List<ByteBuffer> values)
        {
            super(column, operator);
            assert column.type.isMultiCell();
            this.value = operator.isIN() ? ListType.getInstance(column.type, false).pack(values)
                                         : values.get(0);
            if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
                throw invalidRequest("Invalid 'unset' value in condition");
        }

        public boolean appliesTo(Row row)
        {
            MultiElementType<?> type = (MultiElementType<?>) column.type;
            return comparisonOperator.isSatisfiedBy(type, row == null ? null : row.getComplexColumnData(column), value);
        }
    }

    public static class Raw
    {
        private final Term.Raw value;
        private final Terms.Raw inValues;

        // Can be null, only used with the syntax "IF m[e] = ..." (in which case it's 'e')
        private final Term.Raw collectionElement;

        // Can be null, only used with the syntax "IF udt.field = ..." (in which case it's 'field')
        private final FieldIdentifier udtField;

        private final Operator operator;

        private Raw(Term.Raw value, Terms.Raw inValues, Term.Raw collectionElement,
                    FieldIdentifier udtField, Operator op)
        {
            this.value = value;
            this.inValues = inValues;
            this.collectionElement = collectionElement;
            this.udtField = udtField;
            this.operator = op;
        }

        /** A condition on a column. For example: "IF col = 'foo'" */
        public static Raw simpleCondition(Term.Raw value, Operator op)
        {
            return new Raw(value, null, null, null,  op);
        }

        /** An IN condition on a column. For example: "IF col IN ('foo', 'bar', ...)" */
        public static Raw simpleInCondition(Terms.Raw inValues)
        {
            return new Raw(null, inValues, null, null, Operator.IN);
        }

        /** A condition on a collection element. For example: "IF col['key'] = 'foo'" */
        public static Raw collectionCondition(Term.Raw value, Term.Raw collectionElement, Operator op)
        {
            return new Raw(value, null, collectionElement, null, op);
        }

        /** An IN condition on a collection element. For example: "IF col['key'] IN ('foo', 'bar', ...)" */
        public static Raw collectionInCondition(Term.Raw collectionElement, Terms.Raw inValues)
        {
            return new Raw(null, inValues, collectionElement, null, Operator.IN);
        }

        /** A condition on a UDT field. For example: "IF col.field = 'foo'" */
        public static Raw udtFieldCondition(Term.Raw value, FieldIdentifier udtField, Operator op)
        {
            return new Raw(value, null, null, udtField, op);
        }

        /** An IN condition on a collection element. For example: "IF col.field IN ('foo', 'bar', ...)" */
        public static Raw udtFieldInCondition(FieldIdentifier udtField, Terms.Raw inValues)
        {
            return new Raw(null, inValues, null, udtField, Operator.IN);
        }

        public ColumnCondition prepare(String keyspace, ColumnMetadata receiver, TableMetadata cfm)
        {
            if (receiver.type instanceof CounterColumnType)
                throw invalidRequest("Conditions on counters are not supported");

            if (collectionElement != null)
            {
                if (!(receiver.type.isCollection()))
                    throw invalidRequest("Invalid element access syntax for non-collection column %s", receiver.name);

                ColumnSpecification elementSpec, valueSpec;
                switch ((((CollectionType<?>) receiver.type).kind))
                {
                    case LIST:
                        elementSpec = Lists.indexSpecOf(receiver);
                        valueSpec = Lists.valueSpecOf(receiver);
                        break;
                    case MAP:
                        elementSpec = Maps.keySpecOf(receiver);
                        valueSpec = Maps.valueSpecOf(receiver);
                        break;
                    case SET:
                        throw invalidRequest("Invalid element access syntax for set column %s", receiver.name);
                    default:
                        throw new AssertionError();
                }

                validateOperationOnDurations(valueSpec.type);
                return condition(receiver, collectionElement.prepare(keyspace, elementSpec), operator, prepareTerms(keyspace, valueSpec));
            }

            if (udtField != null)
            {
                UserType userType = (UserType) receiver.type;
                int fieldPosition = userType.fieldPosition(udtField);
                if (fieldPosition == -1)
                    throw invalidRequest("Unknown field %s for column %s", udtField, receiver.name);

                ColumnSpecification fieldReceiver = UserTypes.fieldSpecOf(receiver, fieldPosition);
                validateOperationOnDurations(fieldReceiver.type);
                return condition(receiver, udtField, operator, prepareTerms(keyspace, fieldReceiver));
            }

            validateOperationOnDurations(receiver.type);
            return condition(receiver, operator, prepareTerms(keyspace, receiver));
        }

        private Terms prepareTerms(String keyspace, ColumnSpecification receiver)
        {
            checkFalse(operator == Operator.CONTAINS_KEY && !(receiver.type instanceof MapType),
                       "Cannot use CONTAINS KEY on non-map column %s", receiver.name);
            checkFalse(operator == Operator.CONTAINS && !(receiver.type.isCollection()),
                       "Cannot use CONTAINS on non-collection column %s", receiver.name);

            if (operator.isIN())
            {
                return inValues.prepare(keyspace, receiver);
            }

            if (operator == Operator.CONTAINS || operator == Operator.CONTAINS_KEY)
                receiver = ((CollectionType<?>) receiver.type).makeCollectionReceiver(receiver, operator == Operator.CONTAINS_KEY);

            return Terms.of(value.prepare(keyspace, receiver));
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

        public Term.Raw getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        }
    }

    private interface Accessor
    {
        AbstractType<?> type();

        ByteBuffer elementValue(Row row);

        static Accessor forUdtField(ColumnMetadata column, FieldIdentifier field)
        {
            final UserType udtType = (UserType) column.type;
            final AbstractType<?> elementType = udtType.type(udtType.fieldPosition(field));
            return new Accessor()
            {
                @Override
                public AbstractType<?> type()
                {
                    return elementType;
                }

                @Override
                public ByteBuffer elementValue(Row row)
                {
                    if (row == null)
                        return null;

                    ColumnData data = row.getColumnData(column);

                    if (data == null)
                        return null;

                    if (column.type.isMultiCell())
                    {
                        Cell<?> cell = ((ComplexColumnData) data).getCell(udtType.cellPathForField(field));
                        return cell == null ? null : cell.buffer();
                    }

                    return udtType.unpack(((Cell<?>) data).buffer()).get(udtType.fieldPosition(field));
                }
            };
        }

        static Accessor forListElement(ColumnMetadata column, ByteBuffer index)
        {
            checkNotNull(index, "Invalid null value for list element access");
            final ListType<?> listType = (ListType<?>) column.type;
            return new Accessor()
            {
                @Override
                public AbstractType<?> type()
                {
                    return listType.getElementsType();
                }

                @Override
                public ByteBuffer elementValue(Row row)
                {
                    if (row == null)
                        return null;

                    ColumnData data = row.getColumnData(column);

                    if (data == null)
                        return null;

                    int idx = getListIndex(index);

                    if (column.type.isMultiCell())
                    {
                        ComplexColumnData complexColumnData = (ComplexColumnData) data;

                        if (idx >= complexColumnData.cellsCount())
                            return null;

                        Cell<?> cell = complexColumnData.getCellByIndex(idx);
                        return cell == null ? null : cell.buffer();
                    }

                    List<ByteBuffer> cells = listType.unpack(((Cell<?>) data).buffer());
                    return idx >= cells.size() ? null : cells.get(idx);
                }

                private int getListIndex(ByteBuffer index)
                {
                    int idx = ByteBufferUtil.toInt(index);
                    checkFalse(idx < 0, "Invalid negative list index %d", idx);
                    return idx;
                }
            };
        }

        static Accessor forMapElement(ColumnMetadata column, ByteBuffer key)
        {
            checkNotNull(key, "Invalid null value for map element access");
            final MapType<?, ?> mapType = (MapType<?, ?>) column.type;
            return new Accessor()
            {
                @Override
                public AbstractType<?> type()
                {
                    return mapType.getValuesType();
                }

                @Override
                public ByteBuffer elementValue(Row row)
                {
                    if (row == null)
                        return null;

                    ColumnData data = row.getColumnData(column);

                    if (data == null)
                        return null;

                    if (column.type.isMultiCell())
                    {
                        Cell<?> cell = ((ComplexColumnData) data).getCell(CellPath.create(key));
                        return cell == null ? null : cell.buffer();
                    }

                    return mapType.getSerializer().getSerializedValue(((Cell<?>) data).buffer(), key, type());
                }
            };
        }
    }
}
