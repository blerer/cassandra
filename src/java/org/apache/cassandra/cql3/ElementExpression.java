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
import java.util.Objects;

import org.apache.cassandra.cql3.terms.Lists;
import org.apache.cassandra.cql3.terms.Maps;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * An element expression representation in case of element column expression.
 *
 * <p>This class can be modified to add support for more element expressions like range of elements, for example. </p>
 */

public final class ElementExpression
{
    /**
     * Represent the expression kind
     */
    public enum Kind
    {
        /**
         * UDT field expression (e.g. {@code columnA.fieldA})
         */
        UDT_FIELD
        {
            @Override
            public AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns, ElementExpression.Raw element)
            {
                UserType userType = (UserType) columns.get(0).type;
                int fieldPosition = userType.fieldPosition(element.udtField);
                if (fieldPosition == -1)
                    throw invalidRequest("Unknown field " + element.udtField + " for UDT " + columns.get(0).name);

                return userType.fieldType(fieldPosition);
            }

            @Override
            public String toCQLString(String element)
            {
                return '.' + element;
            }

            @Override
            public String toString()
            {
                return "UDT field";
            }
        },
        /**
         * Collection element expression (e.g. {@code columnA[?]})
         */
        COLLECTION_ELEMENT
        {
            @Override
            public AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns, ElementExpression.Raw element)
            {
                CollectionType<?> collectionType = (CollectionType<?>) columns.get(0).type;
                return collectionType.valueComparator();
            }

            @Override
            public String toCQLString(String element)
            {
                return '[' + element + ']';
            }

            @Override
            public String toString()
            {
                return "collection element";
            }
        };

        public abstract AbstractType<?> type(TableMetadata table, List<ColumnMetadata> columns, ElementExpression.Raw element);
        public abstract String toCQLString(String element);
    }

    /**
     * The kind of element expression - udt field or collection element.
     */
    private final ElementExpression.Kind kind;

    /**
     * The field identifier in case of {@code UDT_FIELD} expression,
     * {@code null} otherwise.
     */
    private final FieldIdentifier fieldIdentifier;

    /**
     * The collection element in case of {@code COLLECTION_ELEMENT} expression,
     * {@code null} otherwise.
     */
    private final Term collectionElement;

    ElementExpression(ElementExpression.Kind kind, FieldIdentifier udtField, Term collectionElement)
    {
        this.kind = kind;
        this.fieldIdentifier = udtField;
        this.collectionElement = collectionElement;
    }

    /**
     * Returns the expression kind.
     * @return the expression kind.
     */
    public ElementExpression.Kind kind()
    {
        return kind;
    }

    /**
     * Returns the expression UDT field in case of a UDT field expression.
     * @return the expression UDT field.
     */
    public FieldIdentifier fieldIdentifier()
    {
        return fieldIdentifier;
    }

    /**
     * Returns the expression collection element in case of a collection element expression.
     * In case of maps - this is the map key which we use to access the map value.
     * @return the expression collection element.
     */
    public Term collectionElement()
    {
        return collectionElement;
    }

    public ByteBuffer mapKey(ColumnMetadata column, QueryOptions options)
    {
        ByteBuffer key = collectionElement().bindAndGet(options);
        if (key == null)
            throw invalidRequest("Invalid null map key for column %s", column.name.toCQLString());
        if (key == ByteBufferUtil.UNSET_BYTE_BUFFER)
            throw invalidRequest("Invalid unset map key for column %s", column.name.toCQLString());
        return key;
    }

    @Override
    public String toString()
    {
        return this.kind.toString();
    }

    public static final class Raw
    {
        private final Kind kind;
        private final Term.Raw rawCollectionElement;

        private final FieldIdentifier udtField;

        Raw(Term.Raw collectionElement, FieldIdentifier udtField, Kind kind)
        {
            this.rawCollectionElement = collectionElement;
            this.udtField = udtField;
            this.kind = kind;
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
         * Returns the collection element if this is a collection element expression, {@code null} otherwise.
         * @return rawCollectionElement.
         */
        public Term.Raw rawCollectionElement()
        {
            return rawCollectionElement;
        }

        /**
         * Returns the collection element if this is a collection element expression, {@code null} otherwise.
         * @return rawCollectionElement.
         */
        public FieldIdentifier rawUdtField()
        {
            return udtField;
        }

        /**
         * Bind this {@link Raw} instance to the schema and return the resulting {@link ElementExpression}.
         *
         * @param table      the table schema
         * @param identifier the column identifier
         * @return the {@link ElementExpression} resulting from the schema binding
         */
        ElementExpression prepare(TableMetadata table, ColumnIdentifier identifier)
        {
            if (rawCollectionElement != null)
                return new ElementExpression(Kind.COLLECTION_ELEMENT, null, prepareCollectionElement(table, rawCollectionElement, identifier));

            return new ElementExpression(Kind.UDT_FIELD, udtField, null);
        }

        private Term prepareCollectionElement(TableMetadata table, Term.Raw rawCollectionElement, ColumnIdentifier identifier)
        {
            ColumnSpecification elementSpec;
            ColumnMetadata receiver = table.getExistingColumn(identifier);

            switch ((((CollectionType<?>) receiver.type).kind))
            {
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

        @Override
        public int hashCode()
        {
            return Objects.hash(kind, rawCollectionElement, udtField);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof ElementExpression.Raw))
                return false;

            ElementExpression.Raw r = (ElementExpression.Raw) o;
            return kind == r.kind && Objects.equals(rawCollectionElement, r.rawCollectionElement) && Objects.equals(udtField, r.udtField);
        }

        @Override
        public String toString()
        {
            return this.kind.toString();
        }
    }
}
