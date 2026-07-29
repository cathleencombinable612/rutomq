package com.rutomq.flink;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;

public final class RutomqVerifier {
    private RutomqVerifier() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("expected prepare or verify");
        }
        if ("prepare".equals(args[0])) {
            if (args.length != 5) {
                throw new IllegalArgumentException(
                        "prepare <bootstrap> <input-topic> <output-topic> <partitions>");
            }
            prepare(args[1], args[2], args[3], Integer.parseInt(args[4]));
        } else if ("verify".equals(args[0])) {
            if (args.length != 6) {
                throw new IllegalArgumentException(
                        "verify <bootstrap> <topic> <prefix> <records> <committed-group-or-dash>");
            }
            verify(args[1], args[2], args[3], Integer.parseInt(args[4]), args[5]);
        } else {
            throw new IllegalArgumentException("unknown verifier mode " + args[0]);
        }
    }

    private static void prepare(
            String bootstrap, String inputTopic, String outputTopic, int partitions)
            throws Exception {
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            try {
                admin.createTopics(List.of(
                                new NewTopic(inputTopic, partitions, (short) 1),
                                new NewTopic(outputTopic, partitions, (short) 1)))
                        .all()
                        .get(15, TimeUnit.SECONDS);
            } catch (ExecutionException error) {
                if (!(error.getCause() instanceof TopicExistsException)) {
                    throw error;
                }
            }
        }
        System.out.printf(
                "prepared topics input=%s output=%s partitions=%d%n",
                inputTopic, outputTopic, partitions);
    }

    private static void verify(
            String bootstrap, String topic, String expectedPrefix, int expectedRecords,
            String committedGroup)
            throws Exception {
        Set<String> expected = new HashSet<>();
        for (int index = 0; index < expectedRecords; index++) {
            expected.add(expectedPrefix + index);
        }
        Set<String> actual = new HashSet<>();
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + topic);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            List<TopicPartition> partitions = new ArrayList<>();
            consumer.partitionsFor(topic).forEach(info ->
                    partitions.add(new TopicPartition(topic, info.partition())));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline && actual.size() < expectedRecords) {
                for (ConsumerRecord<String, String> record :
                        consumer.poll(Duration.ofMillis(250))) {
                    if (!actual.add(record.value())) {
                        throw new AssertionError("duplicate committed record " + record.value());
                    }
                }
            }
            for (ConsumerRecord<String, String> record :
                    consumer.poll(Duration.ofSeconds(1))) {
                if (!actual.add(record.value())) {
                    throw new AssertionError("duplicate committed record " + record.value());
                }
            }
        }
        if (!actual.equals(expected)) {
            Set<String> missing = new HashSet<>(expected);
            missing.removeAll(actual);
            Set<String> unexpected = new HashSet<>(actual);
            unexpected.removeAll(expected);
            throw new AssertionError(
                    "record mismatch missing=" + missing + " unexpected=" + unexpected);
        }
        if (!"-".equals(committedGroup)) {
            verifyCommittedOffsets(bootstrap, committedGroup, expectedRecords);
        }
        System.out.printf(
                "verified topic=%s records=%d prefix=%s group=%s%n",
                topic, actual.size(), expectedPrefix, committedGroup);
    }

    private static void verifyCommittedOffsets(
            String bootstrap, String groupId, int expectedRecords) throws Exception {
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            Map<TopicPartition, OffsetAndMetadata> offsets = Map.of();
            long committed = 0;
            while (System.nanoTime() < deadline) {
                offsets = admin.listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata()
                        .get(10, TimeUnit.SECONDS);
                if (!offsets.isEmpty()
                        && offsets.values().stream().allMatch(offset -> offset != null)) {
                    committed =
                            offsets.values().stream().mapToLong(OffsetAndMetadata::offset).sum();
                    if (committed == expectedRecords) {
                        return;
                    }
                }
                Thread.sleep(250);
            }
            if (offsets.size() != 2 || committed <= 0 || committed > expectedRecords) {
                throw new AssertionError(
                        "group " + groupId + " committed invalid checkpoint offsets "
                                + offsets + ", expected total at most " + expectedRecords);
            }
            verifyCheckpointResume(
                    bootstrap, offsets, Math.toIntExact(expectedRecords - committed));
            System.out.printf(
                    "verified checkpoint offsets group=%s committed=%d recoverable=%d%n",
                    groupId, committed, expectedRecords - committed);
        }
    }

    private static void verifyCheckpointResume(
            String bootstrap,
            Map<TopicPartition, OffsetAndMetadata> offsets,
            int expectedRemaining) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "checkpoint-resume-" + System.nanoTime());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        Set<String> recovered = new HashSet<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(offsets.keySet());
            offsets.forEach((partition, offset) -> consumer.seek(partition, offset.offset()));
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (System.nanoTime() < deadline && recovered.size() < expectedRemaining) {
                for (ConsumerRecord<String, String> record :
                        consumer.poll(Duration.ofMillis(250))) {
                    String position = record.topic() + "-" + record.partition()
                            + "@" + record.offset();
                    if (!recovered.add(position)) {
                        throw new AssertionError(
                                "checkpoint resume returned duplicate " + position);
                    }
                }
            }
            for (ConsumerRecord<String, String> record :
                    consumer.poll(Duration.ofSeconds(1))) {
                String position = record.topic() + "-" + record.partition()
                        + "@" + record.offset();
                if (!recovered.add(position)) {
                    throw new AssertionError(
                            "checkpoint resume returned duplicate " + position);
                }
            }
        }
        if (recovered.size() != expectedRemaining) {
            throw new AssertionError(
                    "checkpoint resume returned " + recovered.size()
                            + " records, expected " + expectedRemaining);
        }
    }

    private static Properties adminProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        return properties;
    }
}
