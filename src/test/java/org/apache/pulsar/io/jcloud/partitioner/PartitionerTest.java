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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import junit.framework.TestCase;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jcloud.BlobStoreAbstractConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


/**
 * partitioner unit test.
 */
@RunWith(Parameterized.class)
public class PartitionerTest extends TestCase {

    @Parameterized.Parameter(0)
    public Partitioner<Object> partitioner;

    @Parameterized.Parameter(1)
    public String expected;

    @Parameterized.Parameter(2)
    public String expectedPartitionedPath;

    @Parameterized.Parameter(3)
    public String expectedBaseFileName;

    @Parameterized.Parameter(4)
    public Record<Object> pulsarRecord;

    @Parameterized.Parameters
    public static Object[][] data() {
        BlobStoreAbstractConfig blobStoreAbstractConfig = new BlobStoreAbstractConfig();
        blobStoreAbstractConfig.setTimePartitionDuration("1d");
        blobStoreAbstractConfig.setTimePartitionPattern("yyyy-MM-dd");
        SimplePartitioner<Object> simplePartitioner = new SimplePartitioner<>();
        simplePartitioner.configure(blobStoreAbstractConfig);
        TimePartitioner<Object> dayPartitioner = new TimePartitioner<>();
        dayPartitioner.configure(blobStoreAbstractConfig);

        SimplePartitioner<Object> simplePartitionerIndexOffset = new SimplePartitioner<>();
        blobStoreAbstractConfig = new BlobStoreAbstractConfig();
        blobStoreAbstractConfig.setPartitionerUseIndexAsOffset(true);
        simplePartitionerIndexOffset.configure(blobStoreAbstractConfig);

        BlobStoreAbstractConfig hourConfig = new BlobStoreAbstractConfig();
        hourConfig.setTimePartitionDuration("4h");
        hourConfig.setTimePartitionPattern("yyyy-MM-dd-HH");
        TimePartitioner<Object> hourPartitioner = new TimePartitioner<>();
        hourPartitioner.configure(hourConfig);

        BlobStoreAbstractConfig noPartitionNumberblobStoreAbstractConfig = new BlobStoreAbstractConfig();
        noPartitionNumberblobStoreAbstractConfig.setTimePartitionDuration("1d");
        noPartitionNumberblobStoreAbstractConfig.setTimePartitionPattern("yyyy-MM-dd");
        noPartitionNumberblobStoreAbstractConfig.setWithTopicPartitionNumber(false);
        SimplePartitioner<Object> noPartitionNumberPartitioner = new SimplePartitioner<>();
        noPartitionNumberPartitioner.configure(noPartitionNumberblobStoreAbstractConfig);

        BlobStoreAbstractConfig numberConfig = new BlobStoreAbstractConfig();
        numberConfig.setTimePartitionDuration("7200000");
        numberConfig.setTimePartitionPattern("yyyy-MM-dd-HH");
        TimePartitioner<Object> numberPartitioner = new TimePartitioner<>();
        numberPartitioner.configure(numberConfig);

        BlobStoreAbstractConfig fieldsConfig = new BlobStoreAbstractConfig();
        fieldsConfig.setFieldsPartitionList(Arrays.asList("userId", "region"));
        fieldsConfig.setFieldsPartitionIgnoreMissing(true);
        FieldsPartitioner<Object> fieldsPartitioner = new FieldsPartitioner<>();
        fieldsPartitioner.configure(fieldsConfig);
        return new Object[][]{
                new Object[]{
                        simplePartitioner,
                        "",
                        "public/default/test",
                        "3221225506",
                        getTopic()
                },
                new Object[]{
                        simplePartitionerIndexOffset,
                        "",
                        "public/default/test",
                        "11115506",
                        getTopic()
                },
                new Object[]{
                        dayPartitioner,
                        "2020-09-08",
                        "public/default/test/2020-09-08",
                        "3221225506",
                        getTopic()
                },
                new Object[]{
                        hourPartitioner,
                        "2020-09-08-12",
                        "public/default/test/2020-09-08-12",
                        "3221225506",
                        getTopic()
                },
                new Object[]{
                        fieldsPartitioner,
                        "user1/US",
                        "public/default/test/user1/US",
                        "3221225506",
                        getRecordWith(Map.of("userId", "user1", "region", "US"))
                },
                new Object[]{
                        fieldsPartitioner,
                        "user1",
                        "public/default/test/user1",
                        "3221225506",
                        getRecordWith(Map.of("userId", "user1"))
                },
                new Object[]{
                        simplePartitioner,
                        "",
                        "public/default/test-partition-1",
                        "3221225506",
                        getPartitionedTopic()
                },
                new Object[]{
                        dayPartitioner,
                        "2020-09-08",
                        "public/default/test-partition-1/2020-09-08",
                        "3221225506",
                        getPartitionedTopic()
                },
                new Object[]{
                        hourPartitioner,
                        "2020-09-08-12",
                        "public/default/test-partition-1/2020-09-08-12",
                        "3221225506",
                        getPartitionedTopic()
                },
                new Object[]{
                        fieldsPartitioner,
                        "user1/US",
                        "public/default/test-partition-1/user1/US",
                        "3221225506",
                        getPartitionedRecordWith(Map.of("userId", "user1", "region", "US"))
                },
                new Object[]{
                        noPartitionNumberPartitioner,
                        "",
                        "public/default/test",
                        "3221225506",
                        getPartitionedTopic()
                },
                new Object[]{
                        numberPartitioner,
                        "2020-09-08-14",
                        "public/default/test-partition-1/2020-09-08-14",
                        "3221225506",
                        getPartitionedTopic()
                },
        };
    }

    public static Record<Object> getPartitionedTopic() {
        @SuppressWarnings("unchecked")
        Message<Object> mock = mock(Message.class);
        when(mock.getPublishTime()).thenReturn(1599578218610L);
        when(mock.getMessageId()).thenReturn(new MessageIdImpl(12, 34, 1));
        String topic = TopicName.get("test-partition-1").toString();
        @SuppressWarnings("unchecked")
        Record<Object> mockRecord = mock(Record.class);
        when(mockRecord.getTopicName()).thenReturn(Optional.of(topic));
        when(mockRecord.getPartitionIndex()).thenReturn(Optional.of(1));
        when(mockRecord.getMessage()).thenReturn(Optional.of(mock));
        when(mockRecord.getPartitionId()).thenReturn(Optional.of(String.format("%s-%s", topic, 1)));
        when(mockRecord.getRecordSequence()).thenReturn(Optional.of(3221225506L));
        return mockRecord;
    }

    public static Record<Object> getTopic() {
        @SuppressWarnings("unchecked")
        Message<Object> mock = mock(Message.class);
        when(mock.getPublishTime()).thenReturn(1599578218610L);
        when(mock.getMessageId()).thenReturn(new MessageIdImpl(12, 34, 1));
        when(mock.hasIndex()).thenReturn(true);
        when(mock.getIndex()).thenReturn(Optional.of(11115506L));

        String topic = TopicName.get("test").toString();
        @SuppressWarnings("unchecked")
        Record<Object> mockRecord = mock(Record.class);
        when(mockRecord.getTopicName()).thenReturn(Optional.of(topic));
        when(mockRecord.getPartitionIndex()).thenReturn(Optional.of(1));
        when(mockRecord.getMessage()).thenReturn(Optional.of(mock));
        when(mockRecord.getPartitionId()).thenReturn(Optional.of(String.format("%s-%s", topic, 1)));
        when(mockRecord.getRecordSequence()).thenReturn(Optional.of(3221225506L));
        return mockRecord;
    }

    public static Record<Object> getRecordWith(Map<String, String> recordFields) {
        Record<Object> mockRecord = getTopic();
        GenericRecord innerRecord = getMockGenericRecordWith(recordFields);
        when(mockRecord.getValue()).thenReturn(innerRecord);
        return mockRecord;
    }

    public static Record<Object> getPartitionedRecordWith(Map<String, String> recordFields) {
        Record<Object> mockRecord = getPartitionedTopic();
        GenericRecord innerRecord = getMockGenericRecordWith(recordFields);
        when(mockRecord.getValue()).thenReturn(innerRecord);
        return mockRecord;
    }

    private static GenericRecord getMockGenericRecordWith(Map<String, String> recordFields) {
        GenericRecord genRec = mock(GenericRecord.class);
        for (String key : recordFields.keySet()) {
            String value = recordFields.get(key);
            when(genRec.getField(key)).thenReturn(value);
        }
        return genRec;
    }

    @Test
    public void testEncodePartition() {
        String encodePartition = partitioner.encodePartition(pulsarRecord, System.currentTimeMillis());
        String message = MessageFormat.format("expected: {0}\nactual: {1}", expected, encodePartition);
        Assert.assertEquals(message, expected, encodePartition);
    }

    @Test
    public void testGeneratePartitionedPath() {
        String encodePartition = partitioner.encodePartition(pulsarRecord, System.currentTimeMillis());
        String partitionedPath =
                partitioner.generatePartitionedPath(pulsarRecord.getTopicName().get(), encodePartition);

        String message = MessageFormat.format("expected: {0}\nactual: {1}", expectedPartitionedPath, partitionedPath);
        Assert.assertEquals(message, expectedPartitionedPath, partitionedPath);
    }

    @Test
    public void testBaseFileName() {
        String baseFileName = partitioner.getBaseFileName(pulsarRecord);

        String message = MessageFormat.format("expected: {0}\nactual: {1}", expectedBaseFileName, baseFileName);
        Assert.assertEquals(message, expectedBaseFileName, baseFileName);
    }
}