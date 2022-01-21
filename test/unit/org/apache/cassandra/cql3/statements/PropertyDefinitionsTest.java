/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.cql3.statements;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import org.apache.cassandra.schema.TableParams;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PropertyDefinitionsTest 
{
    enum Option
    {
        KEY_EXISTING,
        KEY_MISSING;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    private static final Set<String> VALID_PROPERTIES = ImmutableSet.copyOf(EnumSet.allOf(Option.class)
                                                                                   .stream()
                                                                                   .map(Object::toString)
                                                                                   .collect(Collectors.toSet()));

    @Test
    public void testGetBooleanExistant()
    {
        PropertyDefinitions pd = newPropertyDefinition("1");
        assertTrue(pd.getBoolean(Option.KEY_EXISTING, false));

        pd = newPropertyDefinition("TrUe");
        assertTrue(pd.getBoolean(Option.KEY_EXISTING, false));

        pd = newPropertyDefinition("YeS");
        assertTrue(pd.getBoolean(Option.KEY_EXISTING, false));

        pd = newPropertyDefinition(" 1");
        assertFalse(pd.getBoolean(Option.KEY_EXISTING, false));

        pd = newPropertyDefinition("true ");
        assertFalse(pd.getBoolean(Option.KEY_EXISTING, false));

        pd = newPropertyDefinition("ye s");
        assertFalse(pd.getBoolean(Option.KEY_EXISTING, false));
    }

    private PropertyDefinitions newPropertyDefinition(String value)
    {
        PropertyDefinitions pd = new PropertyDefinitions(VALID_PROPERTIES);
        pd.addProperty(Option.KEY_EXISTING.toString(), value);
        return pd;
    }

    @Test
    public void testGetBooleanNonexistant()
    {
        PropertyDefinitions pd = new PropertyDefinitions(VALID_PROPERTIES);
        assertFalse(pd.getBoolean(Option.KEY_MISSING, false));
        assertTrue(pd.getBoolean(Option.KEY_MISSING, true));
    }
    
}
