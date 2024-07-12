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
import java.util.Comparator;
import java.util.List;

final class PostQueryOrderingComparators
{
    private static int compare(Comparator<ByteBuffer> comparator, ByteBuffer aValue, ByteBuffer bValue)
    {
        if (aValue == null)
            return bValue == null ? 0 : -1;

        return bValue == null ? 1 : comparator.compare(aValue, bValue);
    }

    public static Comparator<List<ByteBuffer>> singleColumnComparator(int index, Comparator<ByteBuffer> type)
    {
        return (a, b) -> compare(type, a.get(index), b.get(index));
    }

    public static Comparator<List<ByteBuffer>> compositeColumnComparator(List<Integer> indexes, List<Comparator<ByteBuffer>> types)
    {
        return (a, b) -> {
            for (int i = 0; i < indexes.size(); i++)
            {
                int index = indexes.get(i);
                Comparator<ByteBuffer> type = types.get(i);

                int comparison = compare(type, a.get(index), b.get(index));

                if (comparison != 0)
                    return comparison;
            }

            return 0;
        };
    }

    private PostQueryOrderingComparators()
    {
    }
}
