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
package org.apache.cassandra.cql3.algebra.operators;

import org.apache.cassandra.cql3.algebra.RowExpression;
import org.apache.cassandra.cql3.algebra.RowOperator;

/**
 * A select operator.
 */
public class Filter implements RowOperator
{
    private final RowExpression condition;
    
    private final RowOperator input;

    /**
     * @param input
     * @param condition
     */
    public Filter(RowOperator input, RowExpression condition)
    {
        this.input = input;
        this.condition = condition;
    }

    public RowExpression condition()
    {
        return condition;
    }

    /**
     * @return the input
     */
    public RowOperator input()
    {
        return input;
    }
}
