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

/**
 * Enum used to deal with {@code null} values used for marking missing data. When a {@code null} value is involved
 * in a CQL logical expression the result is always {@code UNKNOWN}.
 */
public enum ThreeValued
{
    TRUE,
    FALSE,
    UNKOWN;

    /**
     * Returns the {@code ThreeValued} corresponding to the specified boolean value.
     * @param b the boolean value.
     * @return the {@code ThreeValued} corresponding to the specified boolean value.
     */
    public static ThreeValued of(boolean b)
    {
        return b ? TRUE : FALSE;
    }

    /**
     * Checks if this value is {@code TRUE}.
     * @return {@code true} if this value is {@code TRUE}, {@code false} otherwise.
     */
    public boolean isTrue()
    {
        return this == TRUE;
    }

    /**
     * Checks if this value is not {@code TRUE}.
     * @return {@code true} if this value is either {@code FALSE} or {@code UNKOWN}, {@code false} otherwise.
     */
    public boolean isNotTrue()
    {
        return this != TRUE;
    }
}
