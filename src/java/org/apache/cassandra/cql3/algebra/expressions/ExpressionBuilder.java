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

import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.cassandra.cql3.algebra.RowExpression;


/**
 * 
 */
public class ExpressionBuilder 
{
    /**
     * Boolean and grouping operators used by the builder.
     */
    private enum Operator
    {
        NOT
        {
            public BooleanOperator asBooleanOperator()
            {
                return BooleanOperator.NOT;
            }

            public boolean hasHigherPrecedenceAndAssociativity(Operator operator)
            {
                return true;
            }
        },
        AND
        {
            public BooleanOperator asBooleanOperator()
            {
                return BooleanOperator.AND;
            }

            public boolean hasHigherPrecedenceAndAssociativity(Operator operator)
            {
                return operator == OR;
            }
        },
        OR
        {
            public BooleanOperator asBooleanOperator()
            {
                return BooleanOperator.OR;
            }

            public boolean hasHigherPrecedenceAndAssociativity(Operator operator)
            {
                return false;
            }
        },
        LEFT_PARENTHESE,
        RIGHT_PARENTHESE;

        public BooleanOperator asBooleanOperator()
        {
            throw new UnsupportedOperationException(this + " operator is not a boolean operator.");
        }

        public boolean hasHigherPrecedenceAndAssociativity(Operator operator)
        {
            throw new UnsupportedOperationException(this + " operator is not a boolean operator.");
        }
    }

    /**
     * Stack used to store the operator and parentheses to convert the boolean expression provided as an inflix 
     * expression into a postfix expression.
     */
    private Deque<Operator> operatorStack = new ArrayDeque<>();

    /**
     * Stack used to convert the postfix expression into an expression tree.
     */
    private Deque<RowExpression> elementStack = new ArrayDeque<>();

    public ExpressionBuilder expression(RowExpression expression)
    {
        elementStack.push(expression);
        return this;
    }

    public ExpressionBuilder leftParenthese()
    {
        operatorStack.push(Operator.LEFT_PARENTHESE);
        return this;
    }

    public ExpressionBuilder rightParenthese()
    {
        Operator previous = operatorStack.pop();
        while (previous != Operator.LEFT_PARENTHESE)
        {
            addOperationToExpression(previous.asBooleanOperator());
            previous = operatorStack.pop();
        }

        return this;
    }

    public ExpressionBuilder and()
    {
        return addOperator(Operator.AND);
    }

    public ExpressionBuilder or()
    {
        addOperator(Operator.OR);
        return this;
    }

    public ExpressionBuilder not()
    {
        addOperator(Operator.NOT);
        return this;
    }

    private ExpressionBuilder addOperator(Operator operator)
    {
        if (operatorStack.isEmpty())
        {
            operatorStack.push(operator);
        }
        else
        {
            Operator previous = operatorStack.peek();
            if (previous == Operator.LEFT_PARENTHESE || operator.hasHigherPrecedenceAndAssociativity(previous))
            {
                operatorStack.push(operator);
            }
            else
            {
                do
                {
                    addOperationToExpression(operatorStack.pop().asBooleanOperator());
                    previous = operatorStack.peek();
                }
                while (!operatorStack.isEmpty() && previous != Operator.LEFT_PARENTHESE && operator.hasHigherPrecedenceAndAssociativity(previous));
                operatorStack.push(operator);
            }
        }
        return this;
    }

    /**
     * @param operator
     */
    private void addOperationToExpression(BooleanOperator operator)
    {
        RowExpression rightExpression = elementStack.pop();
        LogicalBooleanOperation operation = operator.isUnaryOperator() ? new LogicalBooleanOperation(operator, rightExpression)
                                                                       : new LogicalBooleanOperation(operator, elementStack.pop(), rightExpression);

        elementStack.push(operation);
    }

    public RowExpression build()
    {
        while (!operatorStack.isEmpty())
        {
            addOperationToExpression(operatorStack.pop().asBooleanOperator());
        }
        return elementStack.pop();
    }
}
