/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.algebra.expressions;

import org.apache.cassandra.cql3.algebra.RowExpression;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * 
 */
public class ExpressionBuilderTest
{
    @Test
    public void buildingCorrectExpressions()
    {
        assertExpression("A", "A");
        assertExpression("A", "(A)");
        assertExpression("AND(A, B)", "A AND B");
        assertExpression("AND(A, B)", "(A AND B)");
        assertExpression("AND(AND(A, B), C)", "A AND B AND C");
        assertExpression("AND(AND(A, B), C)", "(A AND B AND C)");
        assertExpression("OR(AND(A, B), C)", "A AND B OR C");
        assertExpression("OR(A, AND(B, C))", "A OR B AND C");
        assertExpression("OR(OR(A, B), OR(AND(C, D), E))", "A OR B OR C AND D OR E");
        assertExpression("OR(AND(AND(A, B), C), AND(D, E))", "A AND B AND C OR D AND E");
        assertExpression("OR(AND(A, B), AND(C, D))", "(A AND B) OR (C AND D)");
        assertExpression("NOT(A)", "NOT A");
        assertExpression("NOT(NOT(A))", "NOT NOT A");
        assertExpression("NOT(NOT(A))", "NOT (NOT A)");
        assertExpression("AND(OR(A, B), OR(AND(C, D), E))", "(A OR B) AND ((C AND D) OR E)");
        assertExpression("AND(A, AND(OR(B, C), NOT(D)))", "A AND ((B OR C) AND NOT D)");
        assertExpression("AND(OR(A, B), NOT(AND(C, D)))", "(A OR B) AND NOT (C AND D)");
        assertExpression("OR(NOT(A), B)", "NOT A OR B");
        assertExpression("NOT(OR(A, B))", "NOT (A OR B)");
        assertExpression("OR(A, OR(B, OR(C, D)))", "A OR (B OR (C OR D))");
    }

    private static void assertExpression(String expected, String input)
    {
        assertEquals(expected, toExpression(input).toString());
    }

    private static RowExpression toExpression(String s)
    {
        String[] elements = s.trim().split(" ");
        ExpressionBuilder builder = new ExpressionBuilder();
        for (String element : elements)
        {
            switch (element)
            {
                case "AND":
                    builder.and();
                    break;
                case "OR":
                    builder.or();
                    break;
                case "(NOT":
                    builder.leftParenthese();
                case "NOT":
                    builder.not();
                    break;
                default:
                    for (char c : element.toCharArray())
                    {
                        switch (c)
                        {
                            case '(':
                                builder.leftParenthese();
                                break;
                            case ')':
                                builder.rightParenthese();
                                break;
                            default:
                                assert Character.isAlphabetic(c) : "Alaphabetic character but was " + c ;
                                builder.expression(new CharacterExpression(c));
                                break;
                        }
                    }
                    break;
            }
        }
        return builder.build();
    }

    public static class CharacterExpression implements RowExpression
    {
        private char c;

        public CharacterExpression(char c)
        {
            this.c = c;
        }

        public String toString()
        {
            return Character.toString(c);
        }
    }

}
