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
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.cql3.statements.PropertyDefinitions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.KeyspaceParams.Option;
import org.apache.cassandra.schema.ReplicationParams;

public final class KeyspaceAttributes extends PropertyDefinitions
{
    private static final Set<String> VALID_PROPERTIES = ImmutableSet.copyOf(EnumSet.allOf(KeyspaceParams.Option.class)
                                                                                   .stream()
                                                                                   .map(Object::toString)
                                                                                   .collect(Collectors.toSet()));
    
    public KeyspaceAttributes()
    {
        super(VALID_PROPERTIES);
    }

    KeyspaceParams asNewKeyspaceParams()
    {
        if (!hasOperationsFor(Option.REPLICATION))
            throw new ConfigurationException(String.format("Missing mandatory option '%s'", Option.REPLICATION));

        Map<String, String> replicationOptions = getMap(Option.REPLICATION, Collections.emptyMap());

        if (!replicationOptions.isEmpty() && !replicationOptions.containsKey(ReplicationParams.CLASS))
            throw new ConfigurationException("Missing replication strategy class");

        boolean durableWrites = getBoolean(Option.DURABLE_WRITES, KeyspaceParams.DEFAULT_DURABLE_WRITES);
        return KeyspaceParams.create(durableWrites, replicationOptions);
    }

    KeyspaceParams asAlteredKeyspaceParams(KeyspaceParams previous)
    {
        boolean durableWrites = getBoolean(Option.DURABLE_WRITES, previous.durableWrites);
        Map<String, String> replicationOptions = getMap(Option.REPLICATION, previous.replication.asMap());
        return KeyspaceParams.create(durableWrites, replicationOptions);
    }

}
