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
package org.apache.cassandra.cql3.statements.schema;

import java.util.*;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.cql3.statements.PropertyDefinitions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.SyntaxException;

public class IndexAttributes extends PropertyDefinitions
{
    private enum Option
    {
        OPTIONS;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    private static final Set<String> VALID_PROPERTIES = ImmutableSet.of(Option.OPTIONS.toString());

    public boolean isCustom;
    public String customClass;

    public IndexAttributes()
    {
        super(VALID_PROPERTIES);
    }

    public void validate() throws RequestValidationException
    {

        if (isCustom && customClass == null)
            throw new InvalidRequestException("CUSTOM index requires specifiying the index class");

        if (!isCustom && customClass != null)
            throw new InvalidRequestException("Cannot specify index class for a non-CUSTOM index");

        if (!isCustom && hasOperationsFor(Option.OPTIONS))
            throw new InvalidRequestException("Cannot specify options for a non-CUSTOM index");
    }
    public Map<String, String> getOptions() throws SyntaxException
    {
        Map<String, String> options = new HashMap<>(getMap(Option.OPTIONS, Collections.emptyMap()));
        options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, customClass);
        return options;
    }
}
