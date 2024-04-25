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

import java.util.Comparator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Utility class to test {@code Comparable} classes.
 */
public final class ComparableTestUtils
{
    /**
     * Asserts that {@code compareTo} will always return 0 when the specified comparables are compared to each others.
     *
     * @param comparables the comparables
     * @param <T> the comparables type
     */
    @SafeVarargs
    public static <T extends Comparable<T>> void assertCompareToEquality(T... comparables)
    {
        for (int i = 0, m = comparables.length; i < m; i++)
        {
            assertEquals(0, comparables[i].compareTo(comparables[i]));
        }
    }

    /**
     * Asserts that the order in which the comparables have been provided matches the results from {@code compareTo}.
     *
     * @param comparables the comparables in ascending order
     * @param <T> the comparables type
     */
    @SafeVarargs
    public static <T extends Comparable<T>> void assertOrder(T... comparables)
    {
        assertOrder(Comparator.naturalOrder(), comparables);
    }

    /**
     * Asserts that the order in which the objects have been provided matches the results from {@code Comparator#compare}.
     *
     * @param comparator the comparator
     * @param objects the objects in ascending order
     * @param <T> the objects type
     */
    @SafeVarargs
    public static <T> void assertOrder(Comparator<T> comparator, T... objects)
    {
        for (int i = 0, m = objects.length; i < m; i++)
        {
            for (int j = i; j < m; j++)
            {
                if (i == j)
                {
                    assertEquals(0, comparator.compare(objects[i], objects[i]));
                }
                else
                {
                    T smaller = objects[i];
                    T greater = objects[j];
                    assertTrue(greater + " should be greater than " + smaller, comparator.compare(greater, smaller) > 0);
                    assertTrue(smaller + " should be smaller than " + greater, comparator.compare(smaller, greater) < 0);
                }
            }
        }
    }

    private ComparableTestUtils()
    {
    }
}
