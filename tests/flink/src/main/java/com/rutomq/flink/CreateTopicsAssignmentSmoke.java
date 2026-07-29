package com.rutomq.flink;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.PolicyViolationException;

public final class CreateTopicsAssignmentSmoke {
    private CreateTopicsAssignmentSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "CreateTopicsAssignmentSmoke <bootstrap> <topic-prefix>");
        }
        String prefix = args[1];
        String defaults = prefix + "-defaults";
        String manual = prefix + "-manual";
        String validateOnly = prefix + "-validate";

        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(args[0]))) {
            NewTopic defaultsTopic =
                    new NewTopic(defaults, Optional.empty(), Optional.empty())
                            .configs(Map.of("retention.ms", "123"));
            CreateTopicsResult created = admin.createTopics(
                    List.of(defaultsTopic, new NewTopic(manual, assignments(0, 1))));
            created.all().get(15, TimeUnit.SECONDS);
            assertCreateResult(created, defaults, 3, "123");
            assertCreateResult(created, manual, 2, null);
            assertTopology(admin, defaults, 3);
            assertTopology(admin, manual, 2);

            admin.createTopics(
                            List.of(new NewTopic(validateOnly, Optional.empty(), Optional.empty())),
                            new CreateTopicsOptions().validateOnly(true))
                    .all()
                    .get(15, TimeUnit.SECONDS);
            if (admin.listTopics()
                    .names()
                    .get(15, TimeUnit.SECONDS)
                    .contains(validateOnly)) {
                throw new AssertionError("validate-only topic was persisted");
            }

            reject(
                    admin,
                    new NewTopic(prefix + "-zero-partitions", 0, (short) 1),
                    InvalidPartitionsException.class);
            reject(
                    admin,
                    new NewTopic(prefix + "-physical-replicas", 1, (short) 2),
                    InvalidReplicationFactorException.class);
            reject(
                    admin,
                    new NewTopic(prefix + "-assignment-gap", assignments(1)),
                    InvalidReplicaAssignmentException.class);
            reject(
                    admin,
                    new NewTopic(
                            prefix + "-unknown-broker",
                            Map.of(0, List.of(1))),
                    InvalidReplicaAssignmentException.class);
            rejectBatchLimit(admin, prefix);
        }
        System.out.printf(
                "Kafka CreateTopics virtual assignments passed prefix=%s%n", prefix);
    }

    private static void assertCreateResult(
            CreateTopicsResult result, String topic, int partitions, String retention)
            throws Exception {
        if (result.numPartitions(topic).get(15, TimeUnit.SECONDS) != partitions) {
            throw new AssertionError("unexpected CreateTopics partition result for " + topic);
        }
        if (result.replicationFactor(topic).get(15, TimeUnit.SECONDS) != 1) {
            throw new AssertionError("unexpected CreateTopics replication result for " + topic);
        }
        if (Uuid.ZERO_UUID.equals(result.topicId(topic).get(15, TimeUnit.SECONDS))) {
            throw new AssertionError("CreateTopics returned a zero topic ID for " + topic);
        }
        Config config = result.config(topic).get(15, TimeUnit.SECONDS);
        if (config.entries().size() != 19) {
            throw new AssertionError(
                    "unexpected effective config count for " + topic + ": "
                            + config.entries().size());
        }
        ConfigEntry cleanupPolicy = config.get("cleanup.policy");
        if (cleanupPolicy == null
                || cleanupPolicy.source() != ConfigEntry.ConfigSource.DEFAULT_CONFIG
                || cleanupPolicy.isReadOnly()
                || cleanupPolicy.isSensitive()) {
            throw new AssertionError("invalid default config metadata for " + topic);
        }
        if (retention != null) {
            ConfigEntry retentionMs = config.get("retention.ms");
            if (retentionMs == null
                    || !retention.equals(retentionMs.value())
                    || retentionMs.source() != ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
                throw new AssertionError("invalid dynamic config metadata for " + topic);
            }
        }
    }

    private static Map<Integer, List<Integer>> assignments(int... partitions) {
        Map<Integer, List<Integer>> assignments = new LinkedHashMap<>();
        for (int partition : partitions) {
            assignments.put(partition, List.of(0));
        }
        return assignments;
    }

    private static void assertTopology(Admin admin, String topic, int partitions)
            throws Exception {
        TopicDescription description = admin.describeTopics(List.of(topic))
                .allTopicNames()
                .get(15, TimeUnit.SECONDS)
                .get(topic);
        if (description.partitions().size() != partitions) {
            throw new AssertionError(
                    "unexpected partition count for " + topic + ": "
                            + description.partitions().size());
        }
        boolean virtual = description.partitions().stream()
                .allMatch(partition ->
                        partition.replicas().size() == 1
                                && partition.replicas().get(0).id() == 0);
        if (!virtual) {
            throw new AssertionError("unexpected replica assignment for " + topic);
        }
    }

    private static void reject(
            Admin admin, NewTopic topic, Class<? extends Throwable> expected) throws Exception {
        try {
            admin.createTopics(List.of(topic)).all().get(15, TimeUnit.SECONDS);
            throw new AssertionError("invalid topic was accepted: " + topic.name());
        } catch (ExecutionException error) {
            if (!expected.isInstance(error.getCause())) {
                throw new AssertionError(
                        "unexpected error for " + topic.name() + ": " + error.getCause(),
                        error);
            }
        }
    }

    private static void rejectBatchLimit(Admin admin, String prefix) throws Exception {
        String large = prefix + "-batch-limit-large";
        String small = prefix + "-batch-limit-small";
        try {
            admin.createTopics(List.of(
                            new NewTopic(large, 10_000, (short) 1),
                            new NewTopic(small, 1, (short) 1)))
                    .all()
                    .get(15, TimeUnit.SECONDS);
            throw new AssertionError("excessive partition batch was accepted");
        } catch (ExecutionException error) {
            if (!(error.getCause() instanceof PolicyViolationException)) {
                throw new AssertionError(
                        "unexpected partition batch error: " + error.getCause(), error);
            }
        }
        var names = admin.listTopics().names().get(15, TimeUnit.SECONDS);
        if (names.contains(large) || names.contains(small)) {
            throw new AssertionError("partition batch limit allowed a partial topic creation");
        }
    }
}
