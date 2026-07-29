package com.rutomq.flink;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidTimestampException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public final class TopicRecordAdmissionSmoke {
    private static final int MAX_BATCH_BYTES = 512;
    private static final long WIDE_TIMESTAMP_DELTA = (long) Integer.MAX_VALUE + 1;

    private TopicRecordAdmissionSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "TopicRecordAdmissionSmoke <bootstrap> <topic>");
        }
        String bootstrap = args[0];
        String topic = args[1];
        createTopic(bootstrap, topic);
        assertConfigs(bootstrap, topic, "producer", "CreateTime", "1000", "1000");

        byte[] compressible = new byte[8 * 1024];
        Arrays.fill(compressible, (byte) 7);
        try (KafkaProducer<byte[], byte[]> producer = producer(bootstrap, "gzip")) {
            producer.send(new ProducerRecord<>(topic, 0, null, compressible))
                    .get(15, TimeUnit.SECONDS);
        }
        setCompression(bootstrap, topic, "uncompressed");
        try (KafkaProducer<byte[], byte[]> producer = producer(bootstrap, "gzip")) {
            expectFailure(
                    RecordTooLargeException.class,
                    producer.send(new ProducerRecord<>(topic, 0, null, compressible)));
        }
        setCompression(bootstrap, topic, "producer");

        byte[] oversized = new byte[2 * 1024];
        for (int index = 0; index < oversized.length; index++) {
            oversized[index] = (byte) index;
        }
        try (KafkaProducer<byte[], byte[]> producer = producer(bootstrap, "none")) {
            expectFailure(
                    RecordTooLargeException.class,
                    producer.send(new ProducerRecord<>(topic, 1, null, oversized)));
            expectFailure(
                    InvalidTimestampException.class,
                    producer.send(new ProducerRecord<>(
                            topic, 1, System.currentTimeMillis() - 10_000, null, new byte[] {1})));
        }

        setTimestampBeforeMax(bootstrap, topic, WIDE_TIMESTAMP_DELTA + 60_000);
        setCompression(bootstrap, topic, "zstd");
        long latestTimestamp = System.currentTimeMillis();
        long firstTimestamp = latestTimestamp - WIDE_TIMESTAMP_DELTA;
        try (KafkaProducer<byte[], byte[]> producer = producer(bootstrap, "none", 1000)) {
            var first = producer.send(new ProducerRecord<>(
                    topic, 1, firstTimestamp, null, new byte[] {31}));
            var second = producer.send(new ProducerRecord<>(
                    topic, 1, latestTimestamp, null, new byte[] {32}));
            ProducerRecord<byte[], byte[]> duplicateHeadersRecord =
                    new ProducerRecord<>(topic, 1, firstTimestamp - 1, null, new byte[] {33});
            duplicateHeadersRecord.headers().add("duplicate", new byte[] {1});
            duplicateHeadersRecord.headers().add("duplicate", new byte[] {2});
            var third = producer.send(duplicateHeadersRecord);
            producer.flush();
            first.get(15, TimeUnit.SECONDS);
            second.get(15, TimeUnit.SECONDS);
            third.get(15, TimeUnit.SECONDS);
        }
        verifyFetchedWideTimestamps(
                bootstrap, topic, firstTimestamp, latestTimestamp);

        setLogAppendTime(bootstrap, topic);
        assertConfigs(bootstrap, topic, "zstd", "LogAppendTime", "0", "0");
        long appendTime;
        try (KafkaProducer<byte[], byte[]> producer = producer(bootstrap, "none")) {
            appendTime = producer.send(
                            new ProducerRecord<>(topic, 0, 1L, null, new byte[] {9}))
                    .get(15, TimeUnit.SECONDS)
                    .timestamp();
        }
        if (appendTime <= 1) {
            throw new AssertionError("broker did not return LogAppendTime: " + appendTime);
        }
        verifyFetchedLogAppendTime(bootstrap, topic, appendTime);
        System.out.printf(
                "Kafka topic record admission passed topic=%s maxBatch=%d appendTime=%d"
                        + " wideTimestampDelta=%d leaderEpoch=0 duplicateHeaders=2"
                        + " nonMonotonicTimestamps=true topicCompression=zstd%n",
                topic, MAX_BATCH_BYTES, appendTime, WIDE_TIMESTAMP_DELTA);
    }

    private static void createTopic(String bootstrap, String topic) throws Exception {
        NewTopic request = new NewTopic(topic, 2, (short) 1)
                .configs(Map.of(
                        "max.message.bytes", Integer.toString(MAX_BATCH_BYTES),
                        "compression.type", "producer",
                        "message.timestamp.type", "CreateTime",
                        "message.timestamp.before.max.ms", "1000",
                        "message.timestamp.after.max.ms", "1000"));
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(bootstrap))) {
            admin.createTopics(List.of(request)).all().get(15, TimeUnit.SECONDS);
        }
    }

    private static void setLogAppendTime(String bootstrap, String topic) throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        List<AlterConfigOp> changes = List.of(
                set("message.timestamp.type", "LogAppendTime"),
                set("message.timestamp.before.max.ms", "0"),
                set("message.timestamp.after.max.ms", "0"));
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(bootstrap))) {
            admin.incrementalAlterConfigs(Map.of(resource, changes))
                    .all()
                    .get(15, TimeUnit.SECONDS);
        }
    }

    private static void setCompression(String bootstrap, String topic, String compression)
            throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(bootstrap))) {
            admin.incrementalAlterConfigs(
                            Map.of(resource, List.of(set("compression.type", compression))))
                    .all()
                    .get(15, TimeUnit.SECONDS);
        }
    }

    private static void setTimestampBeforeMax(
            String bootstrap, String topic, long beforeMaxMs) throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(bootstrap))) {
            admin.incrementalAlterConfigs(Map.of(
                            resource,
                            List.of(set(
                                    "message.timestamp.before.max.ms",
                                    Long.toString(beforeMaxMs)))))
                    .all()
                    .get(15, TimeUnit.SECONDS);
        }
    }

    private static AlterConfigOp set(String name, String value) {
        return new AlterConfigOp(
                new ConfigEntry(name, value), AlterConfigOp.OpType.SET);
    }

    private static void assertConfigs(
            String bootstrap,
            String topic,
            String compression,
            String type,
            String before,
            String after)
            throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(bootstrap))) {
            Config config = admin.describeConfigs(List.of(resource))
                    .all()
                    .get(15, TimeUnit.SECONDS)
                    .get(resource);
            assertConfig(config, "max.message.bytes", Integer.toString(MAX_BATCH_BYTES));
            assertConfig(config, "compression.type", compression);
            assertConfig(config, "message.timestamp.type", type);
            assertConfig(config, "message.timestamp.before.max.ms", before);
            assertConfig(config, "message.timestamp.after.max.ms", after);
        }
    }

    private static void assertConfig(Config config, String name, String expected) {
        ConfigEntry entry = config.get(name);
        if (entry == null || !expected.equals(entry.value())) {
            throw new AssertionError(
                    "unexpected config " + name + ": " + (entry == null ? null : entry.value()));
        }
    }

    private static KafkaProducer<byte[], byte[]> producer(
            String bootstrap, String compression) {
        return producer(bootstrap, compression, 0);
    }

    private static KafkaProducer<byte[], byte[]> producer(
            String bootstrap, String compression, int lingerMs) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(lingerMs));
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        return new KafkaProducer<>(properties);
    }

    private static void verifyFetchedWideTimestamps(
            String bootstrap,
            String topic,
            long expectedFirstTimestamp,
            long expectedLatestTimestamp) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            TopicPartition partition = new TopicPartition(topic, 1);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long firstTimestamp = Long.MIN_VALUE;
            long latestTimestamp = Long.MIN_VALUE;
            boolean duplicateHeadersSeen = false;
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(250))) {
                    if (record.leaderEpoch().orElse(-1) != 0) {
                        throw new AssertionError(
                                "unexpected fetched leader epoch " + record.leaderEpoch());
                    }
                    if (record.timestampType() != TimestampType.CREATE_TIME) {
                        throw new AssertionError(
                                "unexpected wide-delta timestamp type "
                                        + record.timestampType());
                    }
                    if (record.value().length == 1 && record.value()[0] == 31) {
                        firstTimestamp = record.timestamp();
                    } else if (record.value().length == 1 && record.value()[0] == 32) {
                        latestTimestamp = record.timestamp();
                    } else if (record.value().length == 1 && record.value()[0] == 33) {
                        if (record.timestamp() != expectedFirstTimestamp - 1) {
                            throw new AssertionError(
                                    "non-monotonic timestamp changed: " + record.timestamp());
                        }
                        var headers = record.headers().headers("duplicate").iterator();
                        if (!headers.hasNext()
                                || !Arrays.equals(headers.next().value(), new byte[] {1})
                                || !headers.hasNext()
                                || !Arrays.equals(headers.next().value(), new byte[] {2})
                                || headers.hasNext()) {
                            throw new AssertionError(
                                    "duplicate record headers changed during Produce/Fetch");
                        }
                        duplicateHeadersSeen = true;
                    }
                }
                if (firstTimestamp != Long.MIN_VALUE
                        && latestTimestamp != Long.MIN_VALUE
                        && duplicateHeadersSeen) {
                    if (firstTimestamp != expectedFirstTimestamp
                            || latestTimestamp != expectedLatestTimestamp) {
                        throw new AssertionError(
                                "wide timestamp delta changed: "
                                        + firstTimestamp + " " + latestTimestamp);
                    }
                    return;
                }
            }
        }
        throw new AssertionError("wide timestamp-delta records were not fetched");
    }

    private static void expectFailure(
            Class<? extends Throwable> expected,
            java.util.concurrent.Future<?> result)
            throws Exception {
        try {
            result.get(15, TimeUnit.SECONDS);
            throw new AssertionError("expected " + expected.getSimpleName());
        } catch (ExecutionException error) {
            for (Throwable current = error; current != null; current = current.getCause()) {
                if (expected.isInstance(current)) {
                    return;
                }
            }
            throw error;
        }
    }

    private static void verifyFetchedLogAppendTime(
            String bootstrap, String topic, long expectedTimestamp) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            TopicPartition partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(250))) {
                    if (record.value().length == 1 && record.value()[0] == 9) {
                        if (record.timestampType() != TimestampType.LOG_APPEND_TIME
                                || record.timestamp() != expectedTimestamp) {
                            throw new AssertionError(
                                    "unexpected fetched append timestamp "
                                            + record.timestampType() + " " + record.timestamp());
                        }
                        return;
                    }
                }
            }
        }
        throw new AssertionError("LogAppendTime record was not fetched");
    }
}
