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
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;

/**
 * Partitions by configured list of fields, creates a folder hierarchy in same order.
 *
 * @param <T> expected to be a GenericRecord
 */
public class FieldsPartitioner<T> extends AbstractPartitioner<T> {

    private List<String> fields;
    private boolean ignoreMissing;

    @Override
    public void configure(BlobStoreAbstractConfig config) {
        super.configure(config);
        this.fields = config.getFieldsPartitionList();
        this.ignoreMissing = config.isFieldsPartitionIgnoreMissing();
    }

    @Override
    public String encodePartition(Record<T> sinkRecord) {
        T message = sinkRecord.getValue();
        if (message instanceof GenericRecord) {
            GenericRecord genericRecord = (GenericRecord) message;
            return fields.stream()
                .map(field -> getFieldValue(genericRecord, field))
                .filter(s -> s != null)
                .collect(Collectors.joining(PATH_SEPARATOR));
        } else {
            Class<?> cls = message.getClass();
            throw new IllegalArgumentException("Unknown record type class " + cls + ", expected GenericRecord!");
        }
    }

    private String getFieldValue(GenericRecord genericRecord, String field) {
        Object value = genericRecord.getField(field);
        String valueString = null;
        if (value != null) {
            valueString = value.toString().trim();
        }
        if (StringUtils.isBlank(valueString)) {
            valueString = null;
        }
        if (!ignoreMissing && valueString == null) {
            throw new IllegalArgumentException("Field '" + field + "' not found in record");
        }
        return valueString;
    }

}
