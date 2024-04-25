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

package org.apache.cassandra.utils;

import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;

/**
 * Comparator for enum elements.
 * @param <T> the enum type
 */
public final class EnumComparator<T  extends Enum<T>> implements Comparator<T>
{
    /**
     * The map defining the order of the enum elements.
     */
    private final Map<T, Integer> order;

    /**
     * Creates an {@code EnumComparator} instance that order enume elments according to the provided order.
     * @param order the enum elements in the order that this comparator will enforce.
     */
    @SafeVarargs
    public EnumComparator(T... order)
    {
        if (order.length == 0)
            throw new IllegalArgumentException("No elements specified");

        Class<T> clazz = (Class<T>) order[0].getClass();

        // If the enum elements override some methods we need to go a level higher
        if (!clazz.isEnum())
            clazz = (Class<T>) clazz.getSuperclass();

        T[] elements = clazz.getEnumConstants();

        EnumMap<T, Integer> map = new EnumMap<>(clazz);
        for (T element : elements)
        {
            int i = ArrayUtils.indexOf(order, element);

            if (i == -1)
                throw new IllegalArgumentException(element + " order is not specified");

            map.put(order[i], i);
        }
        this.order = Collections.unmodifiableMap(map);
    }
    @Override
    public int compare(T x, T y)
    {
        return Integer.compare(order.get(x), order.get(y));
    }
}
