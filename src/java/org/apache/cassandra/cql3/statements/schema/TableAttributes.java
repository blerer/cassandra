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
package org.apache.cassandra.cql3.statements.schema;

import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.cql3.statements.PropertyDefinitions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.MemtableParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.TableParams.Option;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public final class TableAttributes extends PropertyDefinitions
{
    private static final TableParams DEFAULT_TABLE_PARAMS = TableParams.builder().build();

    private static final Set<String> VALID_PROPERTIES = ImmutableSet.copyOf(EnumSet.allOf(TableParams.Option.class)
                                                                                   .stream()
                                                                                   .map(Object::toString)
                                                                                   .collect(Collectors.toSet()));

    public TableAttributes()
    {
        super(VALID_PROPERTIES);
    }

    TableParams asNewTableParams()
    {
        return buildAndValidate(DEFAULT_TABLE_PARAMS);
    }

    TableParams asNewTableParamsForMaterializedView()
    {
        if (hasOperationsFor(Option.DEFAULT_TIME_TO_LIVE) && getInt(Option.DEFAULT_TIME_TO_LIVE, 0) != 0)
        {
            throw invalidRequest("Cannot set default_time_to_live for a materialized view. " +
                                 "Data in a materialized view always expire at the same time than " +
                                 "the corresponding data in the parent table.");
        }

        return buildAndValidate(DEFAULT_TABLE_PARAMS);
    }

    TableParams asAlteredTableParams(TableParams previous)
    {
        if (getId().isPresent())
            throw new ConfigurationException("Cannot alter table id.");

        return buildAndValidate(previous);
    }

    private TableParams buildAndValidate(TableParams currentParams)
    {
        TableParams newParams = build(currentParams);
        newParams.validate();
        return newParams;
    }

    public Optional<TableId> getId() throws ConfigurationException
    {
        String id = getString(Option.ID, null);
        try
        {
            return id != null ? Optional.of(TableId.fromString(id)) : Optional.empty();
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException("Invalid table id", e);
        }
    }

    public static Set<String> validKeywords()
    {
        return VALID_PROPERTIES;
    }

    public static Set<String> allKeywords()
    {
        return VALID_PROPERTIES;
    }

    private TableParams build(TableParams previous)
    {
        TableParams.Builder builder = TableParams.builder();

        builder.allowAutoSnapshot(getBoolean(Option.ALLOW_AUTO_SNAPSHOT, previous.allowAutoSnapshot));
        builder.bloomFilterFpChance(getDouble(Option.BLOOM_FILTER_FP_CHANCE, previous.bloomFilterFpChance));

        Map<String, String> cachingOptions = getMap(Option.CACHING, previous.caching.asMap());
        if (!cachingOptions.isEmpty())
            builder.caching(CachingParams.fromMap(cachingOptions));

        builder.comment(getString(Option.COMMENT, previous.comment));

        Map<String, String> compactionOptions = getMap(Option.COMPACTION, previous.compaction.asMap());
        if (!compactionOptions.isEmpty())
            builder.compaction(CompactionParams.fromMap(compactionOptions));

        Map<String, String> compressionOptions = getMap(Option.COMPRESSION, previous.compression.asMap());
        Double crcCheckChance = previous.crcCheckChance;
        if (!compressionOptions.isEmpty())
        {
            //crc_check_chance was "promoted" from a compression property to a top-level-property after #9839
            //so we temporarily accept it to be defined as a compression option, to maintain backwards compatibility
            if (compressionOptions.containsKey(Option.CRC_CHECK_CHANCE.toString().toLowerCase()))
            {
                crcCheckChance = getDeprecatedCrcCheckChance(compressionOptions);
            }
            builder.compression(CompressionParams.fromMap(compressionOptions));
        }

        builder.memtable(getMemtableParams(Option.MEMTABLE, previous.memtable));
        builder.defaultTimeToLive(getInt(Option.DEFAULT_TIME_TO_LIVE, previous.defaultTimeToLive));
        builder.gcGraceSeconds(getInt(Option.GC_GRACE_SECONDS, previous.gcGraceSeconds));
        builder.maxIndexInterval(getInt(Option.MAX_INDEX_INTERVAL, previous.maxIndexInterval));
        builder.memtableFlushPeriodInMs(getInt(Option.MEMTABLE_FLUSH_PERIOD_IN_MS, previous.memtableFlushPeriodInMs));
        builder.minIndexInterval(getInt(Option.MIN_INDEX_INTERVAL, previous.minIndexInterval));
        builder.speculativeRetry(getSpeculativeRetryPolicy(Option.SPECULATIVE_RETRY, previous.speculativeRetry));
        builder.additionalWritePolicy(getSpeculativeRetryPolicy(Option.ADDITIONAL_WRITE_POLICY, previous.additionalWritePolicy));
        builder.crcCheckChance(getDouble(Option.CRC_CHECK_CHANCE, crcCheckChance));
        builder.cdc(getBoolean(Option.CDC, previous.cdc));
        builder.readRepair(getReadRepairStrategy(Option.READ_REPAIR, previous.readRepair));

        return builder.build();
    }

    private MemtableParams getMemtableParams(Option option, MemtableParams previous)
    {
        return MemtableParams.get(getString(option, previous.toString()));
    }
    
    private SpeculativeRetryPolicy getSpeculativeRetryPolicy(Option option, SpeculativeRetryPolicy previous)
    {
        return SpeculativeRetryPolicy.fromString(getString(option, previous.toString()));
    }

    private ReadRepairStrategy getReadRepairStrategy(Option option, ReadRepairStrategy previous)
    {
        return ReadRepairStrategy.fromString(getString(option, previous.toString()));
    }

    private Double getDeprecatedCrcCheckChance(Map<String, String> compressionOpts)
    {
        String value = compressionOpts.get(Option.CRC_CHECK_CHANCE.toString().toLowerCase());
        try
        {
            return Double.valueOf(value);
        }
        catch (NumberFormatException e)
        {
            throw new SyntaxException(String.format("Invalid double value %s for crc_check_chance.'", value));
        }
    }
}
