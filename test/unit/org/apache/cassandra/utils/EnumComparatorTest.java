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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class EnumComparatorTest
{
    private enum TestEnum
    {
        A, B, C, D;
    }

    private enum TestEnumWithOverridenMethods
    {
        A
        {
            @Override
            public String toString()
            {
                return "a";
            }
        },
        B
        {
            @Override
            public String toString()
            {
                return "b";
            }
        },
        C
        {
            @Override
            public String toString()
            {
                return "c";
            }
        },
        D
        {
            @Override
            public String toString()
            {
                return "d";
            }
        },
    }

    @Test
    public void testMissingElements()
    {
        assertThatThrownBy(() -> new EnumComparator<>(TestEnum.D, TestEnum.C, TestEnum.B))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("A order is not specified");

        assertThatThrownBy(() -> new EnumComparator<>())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("No elements specified");
    }

    @Test
    public void testSort()
    {
        TestEnum[] ordered = new TestEnum[] {TestEnum.D, TestEnum.C, TestEnum.B, TestEnum.A};
        Comparator<TestEnum> comparator = new EnumComparator<>(ordered);
        ComparableTestUtils.assertOrder(comparator, ordered);
    }

    @Test
    public void testSortWithEnumWithOverridenMethods()
    {
        TestEnumWithOverridenMethods[] ordered = new TestEnumWithOverridenMethods[] {TestEnumWithOverridenMethods.D,
                                                                                     TestEnumWithOverridenMethods.C,
                                                                                     TestEnumWithOverridenMethods.B,
                                                                                     TestEnumWithOverridenMethods.A};
        Comparator<TestEnumWithOverridenMethods> comparator = new EnumComparator<>(ordered);
        ComparableTestUtils.assertOrder(comparator, ordered);
    }
}
