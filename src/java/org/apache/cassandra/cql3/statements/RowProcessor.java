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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;

public interface RowProcessor<T>
{
    T process(PartitionIterator partitions);

    static RowProcessor<Map<DecoratedKey, List<Row>>>  rawInternalProcessor()
    {
        return partitions -> {
            Map<DecoratedKey, List<Row>> result = Collections.emptyMap();
            while (partitions.hasNext())
            {
                try (RowIterator in = partitions.next())
                {
                    List<Row> out = Collections.emptyList();
                    while (in.hasNext())
                    {
                        switch (out.size())
                        {
                            case 0:  out = Collections.singletonList(in.next()); break;
                            case 1:  out = new ArrayList<>(out);
                            default: out.add(in.next());
                        }
                    }
                    switch (result.size())
                    {
                        case 0:  result = Collections.singletonMap(in.partitionKey(), out); break;
                        case 1:  result = new TreeMap<>(result);
                        default: result.put(in.partitionKey(), out);
                    }
                }
            }
            return result;
        };
    }
}
