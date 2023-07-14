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

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.algebra.RowExpression;

/**
 * 
 */
public class LogicalBooleanOperation implements RowExpression
{
    private final BooleanOperator operator;

    private final List<RowExpression> operands;

    public LogicalBooleanOperation(BooleanOperator operator, RowExpression... operands)
    {
        this(operator, ImmutableList.copyOf(operands));
    }

    /**
     * @param operator
     * @param operands
     */
    public LogicalBooleanOperation(BooleanOperator operator, List<RowExpression> operands)
    {
        assert !operator.isNegation() || operands.size() == 1 : "if the operator is a negation there should be only one operand";

        this.operator = operator;
        this.operands = operands;
    }

    public BooleanOperator operator()
    {
        return operator;
    }
    
    public List<RowExpression> operands()
    {
        return operands;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder().append(operator)
                                                   .append('(');
        Joiner.on(", ").appendTo(builder, operands);
        return builder.append(')').toString();
    }
}
