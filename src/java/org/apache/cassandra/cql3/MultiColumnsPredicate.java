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
import java.util.stream.Collectors;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.Term.MultiColumnRaw;
import org.apache.cassandra.cql3.Term.Raw;
import org.apache.cassandra.cql3.restrictions.MultiColumnRestriction;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * A predicate using the tuple notation, which typically affects multiple columns.
 * Examples:
 * {@code
 *  - SELECT ... WHERE (a, b, c) > (1, 'a', 10)
 *  - SELECT ... WHERE (a, b, c) IN ((1, 2, 3), (4, 5, 6))
 *  - SELECT ... WHERE (a, b) < ?
 *  - SELECT ... WHERE (a, b) IN ?
 * }
 */
public class MultiColumnsPredicate extends ColumnsPredicate
{
    private final List<ColumnIdentifier> entities;

    /** A Tuples.Literal or Tuples.Raw marker */
    private final Term.MultiColumnRaw valuesOrMarker;

    /** A list of Tuples.Literal or Tuples.Raw markers */
    private final List<? extends Term.MultiColumnRaw> inValues;

    private final Tuples.INRaw inMarker;

    private MultiColumnsPredicate(List<ColumnIdentifier> entities, Operator predicateType, Term.MultiColumnRaw valuesOrMarker, List<? extends Term.MultiColumnRaw> inValues, Tuples.INRaw inMarker)
    {
        this.entities = entities;
        this.predicateType = predicateType;
        this.valuesOrMarker = valuesOrMarker;

        this.inValues = inValues;
        this.inMarker = inMarker;
    }

    /**
     * Creates a multi-column EQ, LT, LTE, GT, or GTE predicate.
     * {@code
     * For example: "SELECT ... WHERE (a, b) > (0, 1)"
     * }
     * @param entities the columns on the LHS of the predicate
     * @param predicateType the predicate operator
     * @param valuesOrMarker a Tuples.Literal instance or a Tuples.Raw marker
     * @return a new <code>MultiColumnsPredicate</code> instance
     */
    public static MultiColumnsPredicate createNonInPredicate(List<ColumnIdentifier> entities, Operator predicateType, Term.MultiColumnRaw valuesOrMarker)
    {
        assert predicateType != Operator.IN;
        return new MultiColumnsPredicate(entities, predicateType, valuesOrMarker, null, null);
    }

    /**
     * Creates a multi-column IN predicate with a list of IN values or markers.
     * For example: "SELECT ... WHERE (a, b) IN ((0, 1), (2, 3))"
     * @param entities the columns on the LHS of the predicate
     * @param inValues a list of Tuples.Literal instances or a Tuples.Raw markers
     * @return a new <code>MultiColumnsPredicate</code> instance
     */
    public static MultiColumnsPredicate createInPredicate(List<ColumnIdentifier> entities, List<? extends Term.MultiColumnRaw> inValues)
    {
        return new MultiColumnsPredicate(entities, Operator.IN, null, inValues, null);
    }

    /**
     * Creates a multi-column IN predicate with a marker for the IN values.
     * For example: "SELECT ... WHERE (a, b) IN ?"
     * @param entities the columns on the LHS of the predicate
     * @param inMarker a single IN marker
     * @return a new <code>MultiColumnsPredicate</code> instance
     */
    public static MultiColumnsPredicate createSingleMarkerInPredicate(List<ColumnIdentifier> entities, Tuples.INRaw inMarker)
    {
        return new MultiColumnsPredicate(entities, Operator.IN, null, null, inMarker);
    }

    public List<ColumnIdentifier> getEntities()
    {
        return entities;
    }

    /**
     * For non-IN predicates, returns the Tuples.Literal or Tuples.Raw marker for a single tuple.
     * @return a Tuples.Literal for non-IN predicates or Tuples.Raw marker for a single tuple.
     */
    public Term.MultiColumnRaw getValue()
    {
        return predicateType == Operator.IN ? inMarker : valuesOrMarker;
    }

    public List<? extends Term.Raw> getInValues()
    {
        assert predicateType == Operator.IN;
        return inValues;
    }

    @Override
    public boolean isMultiColumn()
    {
        return true;
    }

    @Override
    protected Restriction newEQRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        List<ColumnMetadata> receivers = receivers(table);
        Term term = toTerm(receivers, getValue(), table.keyspace, boundNames);
        return new MultiColumnRestriction.EQRestriction(receivers, term);
    }

    @Override
    protected Restriction newINRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        List<ColumnMetadata> receivers = receivers(table);
        List<Term> terms = toTerms(receivers, inValues, table.keyspace, boundNames);
        if (terms == null)
        {
            Term term = toTerm(receivers, getValue(), table.keyspace, boundNames);
            return new MultiColumnRestriction.InRestrictionWithMarker(receivers, (AbstractMarker) term);
        }

        if (terms.size() == 1)
            return new MultiColumnRestriction.EQRestriction(receivers, terms.get(0));

        return new MultiColumnRestriction.InRestrictionWithValues(receivers, terms);
    }

    @Override
    protected Restriction newSliceRestriction(TableMetadata table, VariableSpecifications boundNames, Bound bound, boolean inclusive)
    {
        List<ColumnMetadata> receivers = receivers(table);
        Term term = toTerm(receivers(table), getValue(), table.keyspace, boundNames);
        return new MultiColumnRestriction.SliceRestriction(receivers, bound, inclusive, term);
    }

    @Override
    protected Restriction newContainsRestriction(TableMetadata table, VariableSpecifications boundNames, boolean isKey)
    {
        throw invalidRequest("%s cannot be used for multi-column predicates", operator());
    }

    @Override
    protected Restriction newIsNotRestriction(TableMetadata table, VariableSpecifications boundNames)
    {
        // this is currently disallowed by the grammar
        throw new AssertionError(String.format("%s cannot be used for multi-column predicates", operator()));
    }

    @Override
    protected Restriction newLikeRestriction(TableMetadata table, VariableSpecifications boundNames, Operator operator)
    {
        throw invalidRequest("%s cannot be used for multi-column predicates", operator());
    }

    @Override
    protected Term toTerm(List<? extends ColumnSpecification> receivers,
                          Raw raw,
                          String keyspace,
                          VariableSpecifications boundNames) throws InvalidRequestException
    {
        Term term = ((MultiColumnRaw) raw).prepare(keyspace, receivers);
        term.collectMarkerSpecification(boundNames);
        return term;
    }

    protected List<ColumnMetadata> receivers(TableMetadata table) throws InvalidRequestException
    {
        List<ColumnMetadata> names = new ArrayList<>(getEntities().size());
        int previousPosition = -1;
        for (ColumnIdentifier id : getEntities())
        {
            ColumnMetadata def = table.getExistingColumn(id);
            checkTrue(def.isClusteringColumn(), "Multi-column predicates can only be applied to clustering columns but was applied to: %s", def.name);
            checkFalse(names.contains(def), "Column \"%s\" appeared twice in a predicate: %s", def.name, this);

            // check that no clustering columns were skipped
            checkFalse(previousPosition != -1 && def.position() != previousPosition + 1,
                       "Clustering columns must appear in the PRIMARY KEY order in multi-column predicates: %s", this);

            names.add(def);
            previousPosition = def.position();
        }
        return names;
    }

    @Override
    public ColumnsPredicate renameIdentifier(ColumnIdentifier from, ColumnIdentifier to)
    {
        if (!entities.contains(from))
            return this;

        List<ColumnIdentifier> newEntities = entities.stream().map(e -> e.equals(from) ? to : e).collect(Collectors.toList());
        return new MultiColumnsPredicate(newEntities, operator(), valuesOrMarker, inValues, inMarker);
    }

    @Override
    public StringBuilder appendCQL(StringBuilder builder)
    {
        builder.append(Tuples.tupleToString(entities, ColumnIdentifier::toCQLString));
        if (isIN())
        {
            return builder.append(" IN ")
                          .append(inMarker != null ? '?' : Tuples.tupleToString(inValues));
        }
        return builder.append(' ')
                      .append(predicateType)
                      .append(' ')
                      .append(valuesOrMarker);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(predicateType, entities, valuesOrMarker, inValues, inMarker);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof MultiColumnsPredicate))
            return false;

        MultiColumnsPredicate mcr = (MultiColumnsPredicate) o;
        return Objects.equals(entities, mcr.entities)
            && Objects.equals(predicateType, mcr.predicateType)
            && Objects.equals(valuesOrMarker, mcr.valuesOrMarker)
            && Objects.equals(inValues, mcr.inValues)
            && Objects.equals(inMarker, mcr.inMarker);
    }
}
