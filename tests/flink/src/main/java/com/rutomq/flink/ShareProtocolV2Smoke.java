package com.rutomq.flink;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public final class ShareProtocolV2Smoke {
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

    private ShareProtocolV2Smoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException(
                    "ShareProtocolV2Smoke <bootstrap> <topic> <group-prefix> <records>");
        }
        String bootstrap = args[0];
        String topic = args[1];
        String groupPrefix = args[2];
        int records = Integer.parseInt(args[3]);
        verifyRecordLimit(bootstrap, topic, groupPrefix + "-record-limit", records);
        verifyRenew(bootstrap, topic, groupPrefix + "-renew", records);
        System.out.printf(
                "Kafka 4.2 share v2 passed topic=%s recordLimit=3 renew=true lag=true%n",
                topic);
    }

    private static void verifyRecordLimit(
            String bootstrap, String topic, String group, int records) throws Exception {
        TopicPartition partition = new TopicPartition(topic, 0);
        resetOffset(bootstrap, group, partition);
        int consumed = 0;
        try (KafkaShareConsumer<String, String> consumer =
                new KafkaShareConsumer<>(consumerProperties(
                        bootstrap, group, "share-v2-record-limit", 3))) {
            consumer.subscribe(List.of(topic));
            int target = Math.min(records, 12);
            while (consumed < target) {
                ConsumerRecords<String, String> batch = pollNonEmpty(consumer);
                if (batch.count() > 3) {
                    throw new AssertionError(
                            "record_limit returned " + batch.count() + " records");
                }
                for (ConsumerRecord<String, String> record : batch) {
                    consumer.acknowledge(record, AcknowledgeType.ACCEPT);
                    consumed++;
                }
                assertCommitted(consumer.commitSync(REQUEST_TIMEOUT));
            }
        }
        awaitEmptyGroup(bootstrap, group);
        assertOffset(bootstrap, group, partition, consumed, records - consumed);
        deleteGroup(bootstrap, topic, group);
    }

    private static void verifyRenew(
            String bootstrap, String topic, String group, int records) throws Exception {
        TopicPartition partition = new TopicPartition(topic, 0);
        resetOffset(bootstrap, group, partition);
        try (KafkaShareConsumer<String, String> consumer =
                new KafkaShareConsumer<>(consumerProperties(
                        bootstrap, group, "share-v2-renew", 1))) {
            consumer.subscribe(List.of(topic));
            ConsumerRecord<String, String> first = onlyRecord(pollNonEmpty(consumer));
            assertLockTimeout(consumer);
            consumer.acknowledge(first, AcknowledgeType.RENEW);
            assertCommitted(consumer.commitSync(REQUEST_TIMEOUT));

            ConsumerRecord<String, String> renewed = onlyRecord(pollNonEmpty(consumer));
            if (renewed.offset() != first.offset()) {
                throw new AssertionError(
                        "renewed record changed offset from "
                                + first.offset() + " to " + renewed.offset());
            }
            assertLockTimeout(consumer);
            consumer.acknowledge(renewed, AcknowledgeType.ACCEPT);
            assertCommitted(consumer.commitSync(REQUEST_TIMEOUT));
        }
        awaitEmptyGroup(bootstrap, group);
        assertOffset(bootstrap, group, partition, 1L, records - 1L);
        deleteGroup(bootstrap, topic, group);
    }

    private static ConsumerRecords<String, String> pollNonEmpty(
            KafkaShareConsumer<String, String> consumer) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
            if (!batch.isEmpty()) {
                return batch;
            }
        }
        throw new AssertionError("share consumer did not receive records");
    }

    private static ConsumerRecord<String, String> onlyRecord(
            ConsumerRecords<String, String> records) {
        if (records.count() != 1) {
            throw new AssertionError("expected one share record, got " + records.count());
        }
        return records.iterator().next();
    }

    private static void assertLockTimeout(KafkaShareConsumer<String, String> consumer) {
        int timeout = consumer.acquisitionLockTimeoutMs().orElse(-1);
        if (timeout <= 0) {
            throw new AssertionError("missing acquisition lock timeout: " + timeout);
        }
    }

    private static Properties consumerProperties(
            String bootstrap, String group, String clientId, int maxRecords) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(maxRecords));
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        properties.put(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");
        properties.put(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "record_limit");
        return properties;
    }

    private static void resetOffset(
            String bootstrap, String group, TopicPartition partition) throws Exception {
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            admin.alterShareGroupOffsets(group, Map.of(partition, 0L))
                    .all()
                    .get(10, TimeUnit.SECONDS);
        }
    }

    private static void assertOffset(
            String bootstrap,
            String group,
            TopicPartition partition,
            long expectedStart,
            long expectedLag)
            throws Exception {
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            Map<TopicPartition, SharePartitionOffsetInfo> offsets =
                    admin.listShareGroupOffsets(Map.of(
                                    group,
                                    new ListShareGroupOffsetsSpec()
                                            .topicPartitions(List.of(partition))))
                            .partitionsToOffsetInfo(group)
                            .get(10, TimeUnit.SECONDS);
            SharePartitionOffsetInfo info = offsets.get(partition);
            if (info == null
                    || info.startOffset() != expectedStart
                    || info.lag().orElse(-1L) != expectedLag) {
                throw new AssertionError("unexpected share offset: " + info);
            }
        }
    }

    private static void assertCommitted(
            Map<TopicIdPartition, java.util.Optional<org.apache.kafka.common.KafkaException>>
                    results) {
        results.forEach((partition, error) -> error.ifPresent(cause -> {
            throw new AssertionError("share acknowledgement failed for " + partition, cause);
        }));
    }

    private static void awaitEmptyGroup(String bootstrap, String group) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        ShareGroupDescription last = null;
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            while (System.nanoTime() < deadline) {
                last = admin.describeShareGroups(List.of(group))
                        .describedGroups()
                        .get(group)
                        .get(10, TimeUnit.SECONDS);
                if (last.members().isEmpty()) {
                    return;
                }
                Thread.sleep(200);
            }
        }
        throw new AssertionError("share group did not become empty: " + last);
    }

    private static void deleteGroup(String bootstrap, String topic, String group)
            throws Exception {
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            admin.deleteShareGroupOffsets(group, Set.of(topic))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            admin.deleteShareGroups(List.of(group)).all().get(10, TimeUnit.SECONDS);
        }
    }

    private static Properties adminProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        return properties;
    }
}
