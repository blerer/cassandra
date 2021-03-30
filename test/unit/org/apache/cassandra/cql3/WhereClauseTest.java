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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WhereClauseTest
{
    @Test
    public void testParsingAndToCQLString() throws Exception
    {
        checkParsingAndToCQLString("pk = 1 AND c = 2 AND v = 'test' AND m['test'] = 2");
        checkParsingAndToCQLString("\"PK\" = ? AND \"C\" = ? AND \"V\" = ? AND \"M\"[?] = ?");
        checkParsingAndToCQLString("pk = 1 AND (c1, c2) = (2, 3)");
        checkParsingAndToCQLString("\"PK\" = ? AND (\"C1\", \"C2\") = (?, ?)");
        checkParsingAndToCQLString("pk = ? AND c = ? AND v = ?");
        checkParsingAndToCQLString("\"PK\" = ? AND \"C\" = ? AND v = ?");
        checkParsingAndToCQLString("pk IN (?, ?) AND c IN (?, ?, ?) AND v IN (?, ?)");
        checkParsingAndToCQLString("pk IN ? AND c IN ? AND v IN ?");
        checkParsingAndToCQLString("\"PK\" IN ? AND \"C\" IN ? AND \"V\" IN ?");
        checkParsingAndToCQLString("pk = 1 AND (c1, c2) IN ((2, 3), (4, 5))");
        checkParsingAndToCQLString("\"PK\" = ? AND (\"C1\", \"C2\") = ((?, ?), (?, ?))");
        checkParsingAndToCQLString("\"PK\" = ? AND (\"C1\", \"C2\") = (?, ?)");
        checkParsingAndToCQLString("pk > ? AND c >= ? AND c <= ? AND v < ?");
        checkParsingAndToCQLString("\"PK\" > ? AND \"C\" >= ? AND \"C\" <= ? AND \"V\" < ?");
        checkParsingAndToCQLString("pk = 1 AND (c1, c2) > (2, 3) AND (c1, c2) < (4, 5)");
        checkParsingAndToCQLString("\"PK\" = ? AND (\"C1\", \"C2\") <= (?, ?) AND (\"C1\") >= (?)");
        checkParsingAndToCQLString("l CONTAINS ? AND v = ?");
        checkParsingAndToCQLString("\"L\" CONTAINS 'test' AND \"V\" = 6");
        checkParsingAndToCQLString("m CONTAINS KEY ? AND v = ?");
        checkParsingAndToCQLString("\"M\" CONTAINS KEY 'test' AND \"V\" = 6");
        checkParsingAndToCQLString("token(pk1, pk2) = token(1, 2) AND c > 0");
        checkParsingAndToCQLString("token(pk1, pk2) > token(?, ?) AND c >= ?");
        checkParsingAndToCQLString("token(\"PK1\", \"PK2\") > token(?, ?) AND \"C\" >= ?");
        checkParsingAndToCQLString("pk = 1 AND c != 2 AND v != 'test'");
        checkParsingAndToCQLString("\"PK\" = 1 AND \"C\" != 2 AND \"V\" != 'test'");
        checkParsingAndToCQLString("pk IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL");
        checkParsingAndToCQLString("\"PK\" IS NOT NULL AND \"C\" IS NOT NULL AND \"V\" IS NOT NULL");
        checkParsingAndToCQLString("pk = 1 AND c = 2 AND v = 'test'");
        checkParsingAndToCQLString("v LIKE '%test'");
    }

    private void checkParsingAndToCQLString(String cql) throws Exception
    {
        WhereClause clause = WhereClause.parse(cql);
        assertEquals(cql, clause.toCQLString());
    }
}
