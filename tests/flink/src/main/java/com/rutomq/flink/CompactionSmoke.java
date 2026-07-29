package com.rutomq.flink;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class CompactionSmoke {
    private CompactionSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("CompactionSmoke <bootstrap> <topic>");
        }
        String bootstrap = args[0];
        String topic = args[1];
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            NewTopic topicDefinition = new NewTopic(topic, 1, (short) 1)
                    .configs(Map.of(
                            "cleanup.policy", "delete",
                            "min.compaction.lag.ms", "0",
                            "max.compaction.lag.ms", "60000",
                            "min.cleanable.dirty.ratio", "0.0",
                            "delete.retention.ms", "500"));
            admin.createTopics(List.of(topicDefinition)).all().get(15, TimeUnit.SECONDS);
            assertConfig(admin, resource, "max.compaction.lag.ms", "60000");
            assertConfig(admin, resource, "min.cleanable.dirty.ratio", "0.0");
            expectAlterFailure(
                    admin,
                    resource,
                    List.of(set("min.cleanable.dirty.ratio", "1.1")));
        }

        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(producerProperties(bootstrap))) {
            send(producer, topic, "same", "old");
            send(producer, topic, "same", "new");
            send(producer, topic, "other", "keep");
            send(producer, topic, null, "unkeyed");
            try (Admin admin = Admin.create(adminProperties(bootstrap))) {
                alter(admin, resource, List.of(set("cleanup.policy", "compact")));
                assertConfig(admin, resource, "cleanup.policy", "compact");
            }
            try (KafkaProducer<String, String> rejectedProducer =
                    new KafkaProducer<>(producerProperties(bootstrap))) {
                expectKeylessProduceFailure(rejectedProducer, topic);
            }
            waitForSnapshot(
                    bootstrap,
                    topic,
                    4,
                    List.of(
                            expected(1, "same", "new"),
                            expected(2, "other", "keep"),
                            expected(3, null, "unkeyed")));

            try (Admin admin = Admin.create(adminProperties(bootstrap))) {
                alter(
                        admin,
                        resource,
                        List.of(set("min.cleanable.dirty.ratio", "1.0")));
                assertConfig(admin, resource, "min.cleanable.dirty.ratio", "1.0");
            }
            send(producer, topic, "same", null);
            requireSnapshotFor(
                    bootstrap,
                    topic,
                    5,
                    List.of(
                            expected(1, "same", "new"),
                            expected(2, "other", "keep"),
                            expected(3, null, "unkeyed"),
                            expected(4, "same", null)),
                    Duration.ofMillis(1200));
            try (Admin admin = Admin.create(adminProperties(bootstrap))) {
                alter(
                        admin,
                        resource,
                        List.of(set("min.cleanable.dirty.ratio", "0.0")));
            }
            waitForSnapshot(
                    bootstrap,
                    topic,
                    5,
                    List.of(
                            expected(2, "other", "keep"),
                            expected(3, null, "unkeyed")));

            try (Admin admin = Admin.create(adminProperties(bootstrap))) {
                alter(
                        admin,
                        resource,
                        List.of(
                                set("min.cleanable.dirty.ratio", "1.0"),
                                set("max.compaction.lag.ms", "1000")));
            }
            send(producer, topic, "other", "updated");
            requireSnapshotFor(
                    bootstrap,
                    topic,
                    6,
                    List.of(
                            expected(2, "other", "keep"),
                            expected(3, null, "unkeyed"),
                            expected(5, "other", "updated")),
                    Duration.ofMillis(500));
            waitForSnapshot(
                    bootstrap,
                    topic,
                    6,
                    List.of(
                            expected(3, null, "unkeyed"),
                            expected(5, "other", "updated")));
        }
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            alter(
                    admin,
                    resource,
                    List.of(
                            delete("min.cleanable.dirty.ratio"),
                            delete("max.compaction.lag.ms")));
            assertConfig(admin, resource, "min.cleanable.dirty.ratio", "0.5");
            assertConfig(
                    admin,
                    resource,
                    "max.compaction.lag.ms",
                    Long.toString(Long.MAX_VALUE));
        }
        System.out.println(
                "Kafka compacted topic preserved sparse offsets and honored dirty-ratio, "
                        + "maximum-lag, keyless admission, historical keyless retention, "
                        + "and tombstone semantics");
    }

    private static void send(
            KafkaProducer<String, String> producer, String topic, String key, String value)
            throws Exception {
        producer.send(new ProducerRecord<>(topic, 0, key, value))
                .get(15, TimeUnit.SECONDS);
    }

    private static void expectKeylessProduceFailure(
            KafkaProducer<String, String> producer, String topic) throws Exception {
        try {
            send(producer, topic, null, "rejected-after-compaction");
            throw new AssertionError("compacted topic accepted a keyless client record");
        } catch (ExecutionException expected) {
            if (!(expected.getCause() instanceof InvalidRecordException)) {
                throw expected;
            }
        }
    }

    private static void waitForSnapshot(
            String bootstrap,
            String topic,
            long expectedHighWatermark,
            List<ExpectedRecord> expected)
            throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        Snapshot last = null;
        while (System.nanoTime() < deadline) {
            last = snapshot(bootstrap, topic);
            if (last.highWatermark == expectedHighWatermark
                    && recordsMatch(last.records, expected)) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError(
                "compacted snapshot did not converge expected="
                        + expected
                        + " actual="
                        + last);
    }

    private static void requireSnapshotFor(
            String bootstrap,
            String topic,
            long expectedHighWatermark,
            List<ExpectedRecord> expected,
            Duration duration)
            throws Exception {
        long deadline = System.nanoTime() + duration.toNanos();
        while (System.nanoTime() < deadline) {
            Snapshot actual = snapshot(bootstrap, topic);
            if (actual.highWatermark != expectedHighWatermark
                    || !recordsMatch(actual.records, expected)) {
                throw new AssertionError(
                        "compacted snapshot changed before scheduling boundary expected="
                                + expected
                                + " actual="
                                + actual);
            }
            Thread.sleep(100);
        }
    }

    private static void alter(
            Admin admin, ConfigResource resource, List<AlterConfigOp> changes) throws Exception {
        admin.incrementalAlterConfigs(Map.of(resource, changes))
                .all()
                .get(15, TimeUnit.SECONDS);
    }

    private static void expectAlterFailure(
            Admin admin, ConfigResource resource, List<AlterConfigOp> changes) throws Exception {
        try {
            alter(admin, resource, changes);
            throw new AssertionError("invalid compaction configuration was accepted");
        } catch (ExecutionException expected) {
            // Kafka surfaces the broker's per-resource configuration error here.
        }
    }

    private static AlterConfigOp set(String name, String value) {
        return new AlterConfigOp(new ConfigEntry(name, value), AlterConfigOp.OpType.SET);
    }

    private static AlterConfigOp delete(String name) {
        return new AlterConfigOp(new ConfigEntry(name, null), AlterConfigOp.OpType.DELETE);
    }

    private static void assertConfig(
            Admin admin, ConfigResource resource, String name, String expected) throws Exception {
        Config config = admin.describeConfigs(List.of(resource))
                .all()
                .get(15, TimeUnit.SECONDS)
                .get(resource);
        ConfigEntry entry = config.get(name);
        if (entry == null || !expected.equals(entry.value())) {
            throw new AssertionError(
                    "unexpected " + name + " expected=" + expected + " actual=" + entry);
        }
    }

    private static Snapshot snapshot(String bootstrap, String topic) {
        TopicPartition partition = new TopicPartition(topic, 0);
        List<ExpectedRecord> records = new ArrayList<>();
        long highWatermark;
        try (KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(consumerProperties(bootstrap))) {
            consumer.assign(List.of(partition));
            consumer.seek(partition, 0);
            highWatermark = consumer.endOffsets(List.of(partition)).get(partition);
            long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
            while (System.nanoTime() < deadline && consumer.position(partition) < highWatermark) {
                for (ConsumerRecord<String, String> record :
                        consumer.poll(Duration.ofMillis(100))) {
                    records.add(expected(record.offset(), record.key(), record.value()));
                }
            }
        }
        return new Snapshot(highWatermark, records);
    }

    private static boolean recordsMatch(
            List<ExpectedRecord> actual, List<ExpectedRecord> expected) {
        return actual.equals(expected);
    }

    private static ExpectedRecord expected(long offset, String key, String value) {
        return new ExpectedRecord(offset, key, value);
    }

    private static Properties adminProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        return properties;
    }

    private static Properties producerProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "compaction-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000");
        return properties;
    }

    private static Properties consumerProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "compaction-reader-" + System.nanoTime());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        return properties;
    }

    private record ExpectedRecord(long offset, String key, String value) {}

    private record Snapshot(long highWatermark, List<ExpectedRecord> records) {}
}
