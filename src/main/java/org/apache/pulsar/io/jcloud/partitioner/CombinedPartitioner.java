/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.jcloud.partitioner;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;

/**
 * Partitions by configured list of sub-partitioners in given order using their
 * `encodePartition` method.
 *
 * @param <T> expected to be a GenericRecord, configured partitioners has `fields` type
 */
public class CombinedPartitioner<T> extends AbstractPartitioner<T> {

    private List<Partitioner<T>> partitioners;

    @Override
    public void configure(BlobStoreAbstractConfig config) {
        super.configure(config);
        this.partitioners = config.getCombinedPartitionList()
            .stream()
            .map(String::toUpperCase)
            .map(PartitionerType::valueOf)
            .map(partitionerType -> {
                Partitioner<T> partitioner;
                switch (partitionerType) {
                    case TIME:
                        partitioner = new TimePartitioner<>();
                        break;
                    case FIELDS:
                        partitioner = new FieldsPartitioner<>();
                        break;
                    case PARTITION:
                    default:
                        partitioner = new SimplePartitioner<>();
                        break;
                }
                partitioner.configure(config);
                return partitioner;
            })
            .collect(Collectors.toList());
    }

    @Override
    public String encodePartition(Record<T> sinkRecord, long nowInMillis) {
        StringBuilder sb = new StringBuilder();
        for (Partitioner<T> partitioner : this.partitioners) {
            String encoded = partitioner.encodePartition(sinkRecord, nowInMillis);
            if (StringUtils.isNotBlank(encoded)) {
                sb.append(encoded);
                sb.append(PATH_SEPARATOR);
            }
        }
        if (sb.lastIndexOf(PATH_SEPARATOR) == sb.length() - 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    @Override
    public String encodePartition(Record<T> sinkRecord) {
        return encodePartition(sinkRecord, 0L);
    }
}
