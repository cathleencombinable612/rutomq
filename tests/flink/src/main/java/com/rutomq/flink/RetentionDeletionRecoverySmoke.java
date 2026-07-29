package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public final class RetentionDeletionRecoverySmoke {
    private static final short PRODUCE_VERSION = 12;
    private static final String FILE_DELETE_DELAY_MS = "5000";

    private RetentionDeletionRecoverySmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            throw new IllegalArgumentException(
                    "RetentionDeletionRecoverySmoke "
                            + "<prepare|expire|verify-live|verify-expired> "
                            + "<bootstrap> <topic> [second-topic]");
        }
        switch (args[0]) {
            case "prepare" -> {
                if (args.length != 4) {
                    throw new IllegalArgumentException(
                            "prepare requires expired-topic and live-topic");
                }
                prepare(args[1], args[2], args[3]);
            }
            case "expire" -> expire(args[1], args[2]);
            case "verify-live" -> verify(args[1], args[2], false);
            case "verify-expired" -> verify(args[1], args[2], true);
            default -> throw new IllegalArgumentException("unknown mode " + args[0]);
        }
    }

    private static void prepare(String bootstrap, String expiredTopic, String liveTopic)
            throws Exception {
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(bootstrap))) {
            admin.createTopics(List.of(
                            topic(expiredTopic, "0"),
                            topic(liveTopic, "-1")))
                    .all()
                    .get(15, TimeUnit.SECONDS);
        }

        ProduceRequestData data =
                new ProduceRequestData()
                        .setAcks((short) -1)
                        .setTimeoutMs(30_000)
                        .setTopicData(
                                new ProduceRequestData.TopicProduceDataCollection(
                                        List.of(
                                                        produceTopic(
                                                                expiredTopic,
                                                                "expired-value"),
                                                        produceTopic(
                                                                liveTopic,
                                                                "live-value"))
                                                .iterator()));
        ProduceRequest request =
                new ProduceRequest.Builder(PRODUCE_VERSION, PRODUCE_VERSION, data)
                        .build(PRODUCE_VERSION);
        ByteBuffer body = KafkaWire.exchange(bootstrap, request, 201001);
        ProduceResponseData response =
                new ProduceResponseData(new ByteBufferAccessor(body), PRODUCE_VERSION);
        assertProduced(response, expiredTopic);
        assertProduced(response, liveTopic);
        System.out.printf(
                "prepared shared retention object expiredTopic=%s liveTopic=%s%n",
                expiredTopic, liveTopic);
    }

    private static NewTopic topic(String name, String retentionMs) {
        return new NewTopic(name, 1, (short) 1)
                .configs(Map.of(
                        "retention.ms", retentionMs,
                        "file.delete.delay.ms", FILE_DELETE_DELAY_MS));
    }

    private static ProduceRequestData.TopicProduceData produceTopic(
            String topic, String value) {
        long oldTimestamp = System.currentTimeMillis() - 60_000;
        MemoryRecords records =
                MemoryRecords.withRecords(
                        Compression.NONE,
                        new SimpleRecord(
                                oldTimestamp,
                                value.getBytes(StandardCharsets.UTF_8)));
        return new ProduceRequestData.TopicProduceData()
                .setName(topic)
                .setPartitionData(List.of(
                        new ProduceRequestData.PartitionProduceData()
                                .setIndex(0)
                                .setRecords(records)));
    }

    private static void assertProduced(ProduceResponseData response, String topic) {
        for (ProduceResponseData.TopicProduceResponse topicResponse :
                response.responses()) {
            if (!topic.equals(topicResponse.name())) {
                continue;
            }
            if (topicResponse.partitionResponses().size() != 1) {
                throw new AssertionError("unexpected Produce response " + topicResponse);
            }
            ProduceResponseData.PartitionProduceResponse partition =
                    topicResponse.partitionResponses().get(0);
            if (partition.errorCode() != Errors.NONE.code()
                    || partition.baseOffset() != 0) {
                throw new AssertionError(
                        "Produce failed for "
                                + topic
                                + ": "
                                + Errors.forCode(partition.errorCode())
                                + " offset="
                                + partition.baseOffset());
            }
            return;
        }
        throw new AssertionError("missing Produce response for " + topic);
    }

    private static void expire(String bootstrap, String topic) throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        AlterConfigOp operation =
                new AlterConfigOp(
                        new ConfigEntry("retention.ms", "0"),
                        AlterConfigOp.OpType.SET);
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(bootstrap))) {
            admin.incrementalAlterConfigs(Map.of(resource, List.of(operation)))
                    .all()
                    .get(15, TimeUnit.SECONDS);
        }
        System.out.printf("expired retained topic=%s%n", topic);
    }

    private static void verify(String bootstrap, String topic, boolean expired)
            throws Exception {
        TopicPartition partition = new TopicPartition(topic, 0);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class);
        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            long beginning = consumer.beginningOffsets(List.of(partition)).get(partition);
            long end = consumer.endOffsets(List.of(partition)).get(partition);
            long expectedBeginning = expired ? 1 : 0;
            if (beginning != expectedBeginning || end != 1) {
                throw new AssertionError(
                        "unexpected offsets topic="
                                + topic
                                + " beginning="
                                + beginning
                                + " end="
                                + end);
            }
            consumer.seek(partition, beginning);
            var records = consumer.poll(Duration.ofSeconds(2));
            if (expired) {
                if (!records.isEmpty()) {
                    throw new AssertionError("expired topic returned records " + records.count());
                }
            } else {
                if (records.count() != 1) {
                    throw new AssertionError("live topic returned " + records.count() + " records");
                }
                String value =
                        new String(
                                records.iterator().next().value(),
                                StandardCharsets.UTF_8);
                if (!"live-value".equals(value)) {
                    throw new AssertionError("unexpected live value " + value);
                }
            }
        }
        System.out.printf(
                "verified retention topic=%s state=%s%n",
                topic, expired ? "expired" : "live");
    }
}
