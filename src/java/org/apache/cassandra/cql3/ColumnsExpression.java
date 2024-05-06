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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.terms.Lists;
import org.apache.cassandra.cql3.terms.Maps;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkContainsNoDuplicates;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkContainsOnly;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * An expression including one or several columns.
 *
 * <p>This class can be modified to add support for more column expressions like UDT fields, List elements,
 * functions on columns... </p>
 */
public final class ColumnsExpression
{
    /**
     * Represent the expression kind
     */
    public enum Kind
    {
        /**
         * Single column expression (e.g. {@code columnA})
         */
        SINGLE_COLUMN
        {
            @Override
            void validateColumns(TableMetadata table, List<ColumnMetadata> columns)
            {
            }

            @Override
            AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns, FieldIdentifier udtField, Term.Raw collectionElement)
            {
                return columns.get(0).type;
            }

            @Override
            String toCQLString(Stream<String> columns, String udtField, String collectionElement)
            {
                return columns.findFirst().orElseThrow();
            }

            @Override
            public String toString()
            {
                return "single column";
            }
        },
        /**
         * Multi-column expression (e.g. {@code (columnA, columnB)})
         */
        MULTI_COLUMN
        {
            @Override
            protected void validateColumns(TableMetadata table, List<ColumnMetadata> columns)
            {
                int previousPosition = -1;
                for (int i = 0, m = columns.size(); i < m; i++) {
                    ColumnMetadata column = columns.get(i);
                    checkTrue(column.isClusteringColumn(), "Multi-column relations can only be applied to clustering columns but was applied to: %s", column.name);
                    checkFalse(columns.lastIndexOf(column) != i, "Column \"%s\" appeared twice in a relation: %s", column.name, this);

                    // check that no clustering columns were skipped
                    checkFalse(previousPosition != -1 && column.position() != previousPosition + 1,
                            "Clustering columns must appear in the PRIMARY KEY order in multi-column relations: %s", toCQLString(columns, null, null));

                    previousPosition = column.position();
                }
            }

            @Override
            AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns, FieldIdentifier udtField, Term.Raw collectionElement)
            {
                return new TupleType(ColumnMetadata.typesOf(columns));
            }

            @Override
            String toCQLString(Stream<String> columns, String udtField, String collectionElement)
            {
                return columns.collect(Collectors.joining(", ", "(", ")"));
            }

            @Override
            public String toString()
            {
                return "multi-column";
            }
        },
        /**
         * Token expression (e.g. {@code token(columnA, columnB)})
         */
        TOKEN
        {
            @Override
            protected void validateColumns(TableMetadata table, List<ColumnMetadata> columns)
            {
                if (columns.equals(table.partitionKeyColumns()))
                    return;

                // If the columns do not match the partition key columns, let's try to narrow down the problem
                checkTrue(new HashSet<>(columns).containsAll(table.partitionKeyColumns()),
                        "The token() function must be applied to all partition key components or none of them");

                checkContainsNoDuplicates(columns, "The token() function contains duplicate partition key components");

                checkContainsOnly(columns, table.partitionKeyColumns(), "The token() function must contains only partition key components");

                throw invalidRequest("The token function arguments must be in the partition key order: %s",
                                     Joiner.on(", ").join(ColumnMetadata.toIdentifiers(table.partitionKeyColumns())));
            }

            @Override
            AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns, FieldIdentifier udtField, Term.Raw collectionElement)
            {
                return table.partitioner.getTokenValidator();
            }

            @Override
            String toCQLString(Stream<String> columns, String udtField, String collectionElement)
            {
                return columns.collect(Collectors.joining(", ", "token(", ")"));
            }

            @Override
            public String toString()
            {
                return "token";
            }
        },
        /**
         * UDT field element expression
         */
        UDT_FIELD
        {
            @Override
            void validateColumns(TableMetadata table, List<ColumnMetadata> columns)
            {
            }

            @Override
            AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns, FieldIdentifier udtField, Term.Raw collectionElement)
            {
                UserType userType = (UserType) columns.get(0).type;
                int fieldPosition = userType.fieldPosition(udtField);
                if (fieldPosition == -1)
                    throw invalidRequest("Unknown field %s for column %s", udtField, columns.get(0).name);

                return userType.fieldType(fieldPosition);
            }

            @Override
            String toCQLString(Stream<String> columns, String udtField, String collectionElement)
            {
                // KATE fix this later.... for now it is used only in tests anyway
                return new StringBuilder().append(columns.findFirst().orElseThrow())
                                          .append('.')
                                          .append(udtField).toString();
            }

            @Override
            public String toString()
            {
                return "UDT field element";
            }
        },
        /**
         * Collection element expression
         */
        COLLECTION_ELEMENT {
            @Override
            void validateColumns(TableMetadata table, List<ColumnMetadata> columns)
            {
            }

            @Override
            AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns, FieldIdentifier udtField, Term.Raw collectionElement)
            {
                return ((CollectionType<?>) columns.get(0).type).valueComparator();
            }

            @Override
            String toCQLString(Stream<String> columns, String udtField, String collectionElement)
            {
                return new StringBuilder().append(columns.findFirst().orElseThrow())
                                          .append('[')
                                          .append(collectionElement)
                                          .append(']')
                                          .toString();
            }

            @Override
            public String toString()
            {
                return "collection element";
            }
        };


        /**
         * Validates that the specified columns are valid for this kind of expression.
         * @param table the table metadata
         * @param columns the expression column
         */
        abstract void validateColumns(TableMetadata table, List<ColumnMetadata> columns);

        /**
         * Returns the expression type.
         *
         * @param table             the table metadata
         * @param columns           the expression columns
         * @param udtField          the udt field in case of a UDT field expression
         * @param collectionElement the collection element in case of a collection element expression
         * @return the expression type
         */
        abstract AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns, FieldIdentifier udtField, Term.Raw collectionElement);

        /**
         * Returns CQL representation of the expression.
         *
         * @param columns           the expression's columns
         * @param udtField          udt field in case of a UDT field expression
         * @param collectionElement the collection element in case of a collection element expression
         * @return the CQL representation of the expression.
         */
        abstract String toCQLString(Stream<String> columns, String udtField, String collectionElement);

        String toCQLString(List<ColumnMetadata> columns, FieldIdentifier udtField, Term collectionElement)
        {
            CQL3Type type;
            String k = null, u = null;
            switch (this) {
                case COLLECTION_ELEMENT:
                    type = ((CollectionType<?>) columns.get(0).type).valueComparator().asCQL3Type();
                    // If a Term is not terminal it can be a row marker or a function.
                    // We ignore the fact that it could be a function for now.
                    k = collectionElement.isTerminal() ? type.toCQLLiteral(((Term.Terminal) collectionElement).get()) : "?";
                    break;
                case UDT_FIELD:
                    UserType userType = (UserType) columns.get(0).type;
                    int fieldPosition = userType.fieldPosition(udtField);
                    type = userType.fieldType(fieldPosition).asCQL3Type();
                    u = type.toCQLLiteral(udtField.bytes);
                    break;
            }

            return toCQLString(columns.stream().map(c -> c.name.toCQLString()), u, k);
        }

        String toCQLString(List<ColumnIdentifier> identifiers, FieldIdentifier rawUdtField, Term.Raw rawCollectionElement)
        {
            String udtField = rawUdtField == null ? null : rawUdtField.toString();
            String collectionElement = rawCollectionElement == null ? null : rawCollectionElement.getText();
            return toCQLString(identifiers.stream().map(ColumnIdentifier::toCQLString), udtField, collectionElement);
        }
    }

    /**
     * The kind of columns expression.
     */
    private final Kind kind;

    /**
     * The type represented by this expression:
     *  - for a single column the type of the expression will be the one of the column
     *  - for a map element expression the type will be the one of the map value
     *  - for a multi-column expression the type will be a tuple type
     *  - for a collection element expression the type will be the one of the collection elements
     *  - for a UDT field expression the type will be the one of the UDT field
     */
    private final AbstractType<?> type;

    /**
     * The expression columns
     */
    private final List<ColumnMetadata> columns;

    /**
     * The UDT field if this expression is for a UDT field element,
     * {@code null} otherwise.
     */
    private final FieldIdentifier udtField;

    /**
     * The collection element if this expression is for a collection element,
     * {@code null} otherwise.
     */
    private final Term collectionElement;


    private ColumnsExpression(Kind kind, AbstractType<?> type, List<ColumnMetadata> columns, FieldIdentifier udtField, Term collectionElement)
    {
        this.kind = kind;
        this.type = type;
        this.columns = columns;
        this.udtField = udtField;
        this.collectionElement = collectionElement;
    }

    /**
     * Creates an expression for a single column (e.g. {@code columnA}).
     * @param column the column
     * @return an expression for a single column.
     */
    public static ColumnsExpression singleColumn(ColumnMetadata column)
    {
        return new ColumnsExpression(Kind.SINGLE_COLUMN, column.type, ImmutableList.of(column), null, null);
    }

    /**
     * Creates an expression for multi-columns (e.g. {@code (columnA, columnB)}).
     * @param columns the columns
     * @return an expression for multi-columns.
     */
    @VisibleForTesting
    public static ColumnsExpression multiColumns(List<ColumnMetadata> columns)
    {
        AbstractType<?> type = new TupleType(columns.stream()
                                                    .map(c -> c.type)
                                                    .collect(Collectors.toList()));
        return new ColumnsExpression(Kind.MULTI_COLUMN, type, ImmutableList.copyOf(columns),null, null);
    }

    /**
     * Returns the first column metadata.
     * @return the first column metadata.
     */
    public ColumnMetadata firstColumn()
    {
        return columns().get(0);
    }

    /**
     * Returns the last column metadata.
     * @return the last column metadata.
     */
    public ColumnMetadata lastColumn()
    {
        return columns.get(columns.size() - 1);
    }

    /**
     * Returns the columns metadata in position order.
     * @return the columns metadata in position order.
     */
    public List<ColumnMetadata> columns()
    {
        return columns;
    }

    /**
     * Returns the column type.
     * @return the column type.
     */
    public AbstractType<?> type()
    {
        return type;
    }

    /**
     * Returns the column kind (partition key, clustering, static or regular).
     * @return the column kind.
     */
    public ColumnMetadata.Kind columnsKind()
    {
        // All columns must have the same type.
        return firstColumn().kind;
    }

    /**
     * Returns the expression kind.
     * @return the expression kind.
     */
    public Kind kind()
    {
        return kind;
    }

    /**
     * Returns the UDT field in case of UDT_FIELD columns expression.
     * @return the UDT_FIELD expression UDT field.
     */
    public FieldIdentifier udtField()
    {
        return udtField;
    }

    /**
     * Returns the collection element in case of COLLECTION_ELEMENT columns expression.
     * @return the COLLECTION_ELEMENT expression collection element.
     */
    public Term collectionElement()
    {
        return collectionElement;
    }

    public ByteBuffer mapKey(QueryOptions options)
    {
        ByteBuffer key = collectionElement.bindAndGet(options);
        if (key == null)
            throw invalidRequest("Invalid null map key for column %s", firstColumn().name.toCQLString());
        if (key == ByteBufferUtil.UNSET_BYTE_BUFFER)
            throw invalidRequest("Invalid unset map key for column %s", firstColumn().name.toCQLString());
        return key;
    }

    /**
     * Collects the column specifications for the bind variables in the map key.
     * This is obviously a no-op if the expression is not a {@code COLLECTION_ELEMENT} expression.
     *
     * @param boundNames the variables specification where to collect the
     * bind variables of the map key in.
     */
    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        if (collectionElement != null)
            collectionElement.collectMarkerSpecification(boundNames);

    }

    /**
     * Checks if this instance is a column level expression (single or multi-column expression).
     * @return {@code true} if this instance is a column level expression, {@code false} otherwise.
     */
    public boolean isColumnLevelExpression()
    {
        return kind == Kind.SINGLE_COLUMN || kind == Kind.MULTI_COLUMN;
    }

    /**
     * Adds all functions (native and user-defined) used by any component of the restriction
     * to the specified list.
     * @param functions the list to add to
     */
    public void addFunctionsTo(List<Function> functions)
    {
        if (collectionElement != null)
            collectionElement.addFunctionsTo(functions);
    }

    /**
     * Returns CQL representation of this expression.
     * @return the CQL representation of this expression.
     */
    public String toCQLString()
    {
        return kind.toCQLString(columns, udtField, collectionElement);
    }

    @Override
    public String toString()
    {
        String prefix = kind == Kind.SINGLE_COLUMN ? "column "
                                                   : kind == Kind.MULTI_COLUMN ? "tuple " : "";

        return prefix + toCQLString();
    }

    /**
     * Returns the column specification corresponding to this expression.
     * @return the column specification corresponding to this expression.
     */
    public ColumnSpecification columnSpecification()
    {
        ColumnMetadata column = firstColumn();
        return kind == Kind.SINGLE_COLUMN ? column
                                          : new ColumnSpecification(column.ksName, column.cfName, new ColumnIdentifier(toCQLString(), true), type) ;
    }

    /**
     * The parsed version of the {@code ColumnsExpression} as outputed by the CQL parser.
     * {@code Raw.prepare} will be called upon schema binding to create the {@code ColumnsExpression}.
     */
    public static final class Raw
    {
        private final Kind kind;

        /**
         * The columns identifiers
         */
        private final List<ColumnIdentifier> identifiers;

        private final FieldIdentifier rawUdtField;

        private final Term.Raw rawCollectionElement;

        private Raw(Kind kind, List<ColumnIdentifier> identifiers, FieldIdentifier udtField, Term.Raw collectionElement)
        {
            this.kind = kind;
            this.identifiers = identifiers;
            this.rawUdtField = udtField;
            this.rawCollectionElement = collectionElement;
        }

        /**
         * Returns the expression kind.
         * @return the expression kind.
         */
        public Kind kind()
        {
            return kind;
        }

        /**
         * Creates a raw expression for a single column (e.g. {@code columnA}).
         * @param identifier the column identifier
         * @return a raw expression for a single column.
         */
        public static Raw singleColumn(ColumnIdentifier identifier)
        {
            return new Raw(Kind.SINGLE_COLUMN, ImmutableList.of(identifier), null, null);
        }

        /**
         * Creates a raw expression for multi-column (e.g. {@code (columnA, columnB)}).
         * @param identifiers the columns identifier
         * @return a raw expression for multi-column.
         */
        public static Raw multiColumn(List<ColumnIdentifier> identifiers)
        {
            return new Raw(Kind.MULTI_COLUMN, identifiers, null, null);
        }

        /**
         * Creates a raw expression for token restrictions (e.g. {@code token(columnA, columnB)}).
         * @param identifiers the columns identifiers
         * @return a raw token expression.
         */
        public static Raw token(List<ColumnIdentifier> identifiers)
        {
            return new Raw(Kind.TOKEN, identifiers, null, null);
        }

        /**
         * Creates a raw expression for a collection element conditions (e.g. {@code columnA[?]}).
         * @param identifier the collection element column identifier
         * @param rawCollectionElement the raw collection element
         * @return a raw element expression.
         */
        public static Raw collectionElement(ColumnIdentifier identifier, Term.Raw rawCollectionElement)
        {
            return new Raw(Kind.COLLECTION_ELEMENT, ImmutableList.of(identifier), null, rawCollectionElement);
        }

        /**
         * Creates a raw expression for a UDT field conditions.
         * @param identifier the UDT field column identifier
         * @param rawUdtField the raw UDT field
         * @return a raw element expression.
         */
        public static Raw udtField(ColumnIdentifier identifier, FieldIdentifier rawUdtField)
        {
            return new Raw(Kind.UDT_FIELD, ImmutableList.of(identifier), rawUdtField, null);
        }

        /**
         * Renames an identifier in this expression, if applicable.
         * @param from the old identifier
         * @param to the new identifier
         * @return this object, if the old identifier is not in the set of identifiers that this expression covers; otherwise
         *         a new Raw expression with "from" replaced by "to" is returned.
         */
        public Raw renameIdentifier(ColumnIdentifier from, ColumnIdentifier to)
        {
            if (!identifiers.contains(from))
                return this;

            List<ColumnIdentifier> newIdentifiers = identifiers.stream()
                                                               .map(e -> e.equals(from) ? to : e)
                                                               .collect(Collectors.toList());
            return new Raw(kind, newIdentifiers, rawUdtField, rawCollectionElement);
        }

        /**
         * Bind this {@code Raw} instance to the schema and return the resulting {@code ColumnsExpression}.
         *
         * @param table the table schema
         * @return the {@code ColumnsExpression} resulting from the schema binding
         */
        public ColumnsExpression prepare(TableMetadata table)
        {
            List<ColumnMetadata> columns = getColumnsMetadata(table, identifiers);
            kind.validateColumns(table, columns);
            Term collectionElement = prepareCollectionElement(table, rawCollectionElement);
            AbstractType<?> type = kind.type(table, columns, rawUdtField, rawCollectionElement);
            return new ColumnsExpression(kind, type, columns, rawUdtField, collectionElement);
        }

        private Term prepareCollectionElement(TableMetadata table, Term.Raw rawCollectionElement) {
            if (kind != Kind.COLLECTION_ELEMENT)
                return null;

            ColumnSpecification elementSpec;
            ColumnMetadata receiver = table.getExistingColumn(identifiers.get(0));

            switch ((((CollectionType<?>) receiver.type).kind)) {
                case LIST:
                    elementSpec = Lists.indexSpecOf(receiver);
                    break;
                case MAP:
                    elementSpec = Maps.keySpecOf(receiver);
                    break;
                case SET:
                    throw invalidRequest("Invalid element access syntax for set column %s", receiver.name);
                default:
                    throw new AssertionError();
            }

            return rawCollectionElement.prepare(table.keyspace, elementSpec);
        }

        /**
         * Returns the columns corresponding to the identifiers.
         *
         * @param table the table metadata
         * @return the definition of the columns to which apply the token restriction.
         * @throws InvalidRequestException if the entity cannot be resolved
         */
        private static List<ColumnMetadata> getColumnsMetadata(TableMetadata table, List<ColumnIdentifier> identifiers)
        {
            List<ColumnMetadata> columns = new ArrayList<>(identifiers.size());
            for (ColumnIdentifier id : identifiers)
                columns.add(table.getExistingColumn(id));
            return columns;
        }

        /**
         * Returns the columns' identifiers.
         * @return identifiers.
         */
        public List<ColumnIdentifier> identifiers()
        {
            return identifiers;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(kind, identifiers);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof Raw))
                return false;

            Raw r = (Raw) o;
            return kind == r.kind && Objects.equals(identifiers, r.identifiers);
        }

        /**
         * Returns CQL representation of this raw expression.
         * @return the CQL representation of this raw expression.
         */
        public String toCQLString()
        {
            return kind.toCQLString(identifiers, rawUdtField, rawCollectionElement);
        }
    }
}
