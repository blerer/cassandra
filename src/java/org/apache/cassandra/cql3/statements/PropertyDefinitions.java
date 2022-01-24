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
package org.apache.cassandra.cql3.statements;

import java.util.*;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.SyntaxException;

public class PropertyDefinitions
{
    /**
     * An operation that should be applied to a property value.
     *
     * @param <T> the property value type
     */
    public interface PropertyOperation<T>
    {
        /**
         * Applies the operation to the property value
         */
        T apply(T t);

        /**
         * Compose this operation with the previous one 
         * 
         * @param before the previous operation that should be applied on the property value
         * @return a new operation
         */
        PropertyOperation<?> compose(PropertyOperation<?> before);
    };

    /**
     * An operation that fully override the original property value 
     *
     * @param <T> the property value type
     */
    public static final class SetOperation<T> implements PropertyOperation<T>
    {
        /**
         * The property
         */
        private final String name;

        /**
         * The new value of the property
         */
        private final T value;

        public SetOperation(String name, T value)
        {
            this.name = name;
            this.value = value;
        }

        @Override
        public T apply(T t)
        {
            return value;
        }

        @Override
        public PropertyOperation<T> compose(PropertyOperation<?> before)
        {
              if (before instanceof SetOperation)
                  throw new SyntaxException(String.format("Multiple definition for property '%s'", name));

              throw new SyntaxException(String.format("Cannot perform set and update operation on the same property '%s'", name));
        }
    }

    /**
     * An operation that put and remove some new key-values into a Map property 
     */
    public static final class MapValuesOperation implements PropertyOperation<Map<String, String>>
    {
        /**
         * The property name
         */
        private final String name;

        /**
         * The new key-values to put in the property value
         */
        private final Map<String, String> toPut;

        /**
         * The new key-values to remove from the property value
         */
        private final Set<String> toRemove;

        public MapValuesOperation(String name, Map<String, String> toPut, Set<String> toRemove)
        {
            this.name = name;
            this.toPut = toPut;
            this.toRemove = toRemove;
        }

        @Override
        public Map<String, String> apply(Map<String, String> t)
        {
            Map<String, String> copy = new HashMap<>(t);
            copy.putAll(toPut);
            toRemove.forEach(copy::remove);
            return copy;
        }

        @Override
        public PropertyOperation<?> compose(PropertyOperation<?> before)
        {
            if (before instanceof SetOperation)
                throw new SyntaxException(String.format("Cannot perform set and update operation on the same property '%s'", name));

            MapValuesOperation beforeOperation = (MapValuesOperation) before;

            Map<String, String> newToPut = new HashMap<>(beforeOperation.toPut);
            newToPut.putAll(toPut);

            Set<String> newToRemove = new HashSet<>(beforeOperation.toRemove);
            newToRemove.addAll(toRemove);

            return new MapValuesOperation(name, newToPut, newToRemove);
        }
    }

    protected static final Logger logger = LoggerFactory.getLogger(PropertyDefinitions.class);

    private static final Pattern PATTERN_POSITIVE = Pattern.compile("(1|true|yes)");

    private final Set<String> validProperties;

    private final Set<String> obsoleteProperties;

    private final Map<String, PropertyOperation<?>> operations = new HashMap<>();

    public PropertyDefinitions(Set<String> validProperties)
    {
        this(validProperties, ImmutableSet.of());
    }

    public PropertyDefinitions(Set<String> validProperties, Set<String> obsoleteProperties)
    {
        this.validProperties = validProperties;
        this.obsoleteProperties = obsoleteProperties;
    }

    public void addProperty(String name, String value) throws SyntaxException
    {
        validate(name);
        operations.merge(name, new SetOperation<>(name, value), PropertyDefinitions::compose);
    }

    public void addProperty(String name, Map<String, String> value) throws SyntaxException
    {
        validate(name);
        operations.merge(name, new SetOperation<>(name, value), PropertyDefinitions::compose);
    }

    private void validate(String name)
    {
        if (validProperties.contains(name))
            return;

        if (obsoleteProperties.contains(name))
            logger.warn("Ignoring obsolete property {}", name);
        else
            throw new SyntaxException(String.format("Unknown property '%s'", name));
    }

    /**
     * Returns the name of all the properties that are updated by this object.
     */
    public Set<String> updatedProperties()
    {
        return operations.keySet();
    }

    public void removeProperty(String name)
    {
        operations.remove(name);
    }

    protected String getString(Enum<?> property, String currentValue)
    {
        PropertyOperation<String> operation = getOperation(property);
        return operation == null ? currentValue : operation.apply(currentValue);
    }

    protected Map<String, String> getMap(Enum<?> property, Map<String, String> currentValue)
    {
        PropertyOperation<Map<String, String>> operation = getOperation(property);

        return operation == null ? currentValue : operation.apply(currentValue);
    }

    protected boolean getBoolean(Enum<?> property, boolean currentValue)
    {
        String value = getString(property, Boolean.toString(currentValue));
        return PATTERN_POSITIVE.matcher(value.toLowerCase()).matches();
    }

    protected int getInt(Enum<?> property, int currentValue)
    {
        PropertyOperation<String> operation = getOperation(property);
        return operation == null ? currentValue : toInt(property, operation.apply(null));
    }

    protected double getDouble(Enum<?> property, double currentValue)
    {
        PropertyOperation<String> operation = getOperation(property);
        return operation == null ? currentValue : toDouble(property, operation.apply(null));
    }

    private <T> PropertyOperation<T> getOperation(Enum<?> property)
    {
        return (PropertyOperation<T>) operations.get(property.toString());
    }

    private static PropertyOperation<?> compose(PropertyOperation<?> before, PropertyOperation<?> after)
    {
        return after.compose(before);
    }
    
    public boolean hasOperationsFor(Enum<?> property)
    {
        return operations.containsKey(property.toString());
    }

    private static int toInt(Enum<?> property, String value)
    {
        try
        {
            return Integer.parseInt(value);
        }
        catch (NumberFormatException e)
        {
            throw new SyntaxException(String.format("Invalid integer value %s for '%s'", value, property));
        }
    }

    private static double toDouble(Enum<?> property, String value)
    {
        try
        {
            return Double.parseDouble(value);
        }
        catch (NumberFormatException e)
        {
            throw new SyntaxException(String.format("Invalid double value %s for '%s'", value, property));
        }
    }
}

