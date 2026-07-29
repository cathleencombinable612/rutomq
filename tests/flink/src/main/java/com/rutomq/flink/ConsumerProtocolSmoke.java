package com.rutomq.flink;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListConfigResourcesOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ClassicGroupState;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ElectionNotNeededException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class ConsumerProtocolSmoke {
    private ConsumerProtocolSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4 && args.length != 5) {
            throw new IllegalArgumentException(
                    "ConsumerProtocolSmoke <bootstrap> <topic> <group> <records> [metrics-url]");
        }
        String bootstrap = args[0];
        String topic = args[1];
        String group = args[2];
        int records = Integer.parseInt(args[3]);
        String metricsUrl = args.length == 5 ? args[4] : null;
        createTopic(bootstrap, topic);

        CountDownLatch assigned = new CountDownLatch(2);
        CountDownLatch completed = new CountDownLatch(2);
        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Set<String> actual = ConcurrentHashMap.newKeySet();
        Set<String> workersWithRecords = ConcurrentHashMap.newKeySet();
        Thread first = worker(
                "consumer-a", bootstrap, topic, group, records, assigned, completed, stop,
                failure, actual, workersWithRecords);
        Thread second = worker(
                "consumer-b", bootstrap, topic, group, records, assigned, completed, stop,
                failure, actual, workersWithRecords);
        first.start();
        second.start();

        if (!assigned.await(30, TimeUnit.SECONDS)) {
            stop.set(true);
            throw new AssertionError("consumer protocol did not assign both members: " + describe(
                    bootstrap, group));
        }
        awaitStableGroup(bootstrap, group, 2);
        produce(bootstrap, topic, records);

        long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
        while (System.nanoTime() < deadline && actual.size() < records && failure.get() == null) {
            Thread.sleep(100);
        }
        if (metricsUrl != null && actual.size() == records && failure.get() == null) {
            MetricsAcceptance.awaitConsumerSnapshot(metricsUrl, group, topic);
        }
        stop.set(true);
        completed.await(15, TimeUnit.SECONDS);
        first.join(5_000);
        second.join(5_000);
        if (failure.get() != null) {
            throw new AssertionError("consumer worker failed", failure.get());
        }
        Set<String> expected = new HashSet<>();
        for (int index = 0; index < records; index++) {
            expected.add("consumer-" + index);
        }
        if (!actual.equals(expected)) {
            Set<String> missing = new HashSet<>(expected);
            missing.removeAll(actual);
            throw new AssertionError("consumer protocol missing records " + missing);
        }
        if (workersWithRecords.size() != 2) {
            throw new AssertionError(
                    "expected both members to consume a partition, got " + workersWithRecords);
        }
        verifyCommittedRestart(bootstrap, topic, group);
        verifyAdminApis(bootstrap, topic, group);
        System.out.printf(
                "Kafka consumer protocol passed group=%s members=2 records=%d%n", group, records);
    }

    private static Thread worker(
            String workerId,
            String bootstrap,
            String topic,
            String group,
            int records,
            CountDownLatch assigned,
            CountDownLatch completed,
            AtomicBoolean stop,
            AtomicReference<Throwable> failure,
            Set<String> actual,
            Set<String> workersWithRecords) {
        return new Thread(() -> {
            boolean reportedAssignment = false;
            try (KafkaConsumer<String, String> consumer =
                    new KafkaConsumer<>(consumerProperties(bootstrap, group, workerId))) {
                consumer.subscribe(List.of(topic));
                while (!stop.get() && failure.get() == null) {
                    var batch = consumer.poll(Duration.ofMillis(200));
                    if (!reportedAssignment && !consumer.assignment().isEmpty()) {
                        reportedAssignment = true;
                        assigned.countDown();
                    }
                    for (ConsumerRecord<String, String> record : batch) {
                        if (!actual.add(record.value())) {
                            throw new AssertionError("duplicate record " + record.value());
                        }
                        workersWithRecords.add(workerId);
                    }
                    if (!batch.isEmpty()) {
                        consumer.commitSync();
                    }
                }
            } catch (Throwable error) {
                failure.compareAndSet(null, error);
                stop.set(true);
            } finally {
                completed.countDown();
            }
        }, workerId);
    }

    static void createTopic(String bootstrap, String topic) throws Exception {
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            try {
                admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1)))
                        .all()
                        .get(15, TimeUnit.SECONDS);
            } catch (java.util.concurrent.ExecutionException error) {
                if (!(error.getCause() instanceof TopicExistsException)) {
                    throw error;
                }
            }
        }
    }

    private static void produce(String bootstrap, String topic, int records) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int index = 0; index < records; index++) {
                producer.send(new ProducerRecord<>(
                        topic, index % 2, Integer.toString(index), "consumer-" + index));
            }
            producer.flush();
        }
    }

    private static void awaitStableGroup(String bootstrap, String group, int members)
            throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            ConsumerGroupDescription description = describe(bootstrap, group);
            if (description != null
                    && description.groupState() == GroupState.STABLE
                    && description.members().size() == members) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("consumer group did not become stable: " + describe(
                bootstrap, group));
    }

    private static ConsumerGroupDescription describe(String bootstrap, String group)
            throws Exception {
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            return admin.describeConsumerGroups(List.of(group))
                    .describedGroups()
                    .get(group)
                    .get(10, TimeUnit.SECONDS);
        } catch (Exception error) {
            return null;
        }
    }

    private static void verifyCommittedRestart(String bootstrap, String topic, String group) {
        try (KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(consumerProperties(bootstrap, group, "restart"))) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(8).toNanos();
            long assignedAt = -1;
            int records = 0;
            while (System.nanoTime() < deadline) {
                records += consumer.poll(Duration.ofMillis(250)).count();
                if (assignedAt < 0 && !consumer.assignment().isEmpty()) {
                    assignedAt = System.nanoTime();
                }
                if (assignedAt > 0
                        && System.nanoTime() - assignedAt > Duration.ofSeconds(3).toNanos()) {
                    break;
                }
            }
            if (records != 0) {
                throw new AssertionError("committed restart replayed " + records + " records");
            }
        }
    }

    private static void verifyAdminApis(String bootstrap, String topic, String group)
            throws Exception {
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            var cluster = admin.describeCluster();
            if (!"rutomq-cluster".equals(
                    cluster.clusterId().get(10, TimeUnit.SECONDS))) {
                throw new AssertionError("unexpected cluster id");
            }
            if (cluster.nodes().get(10, TimeUnit.SECONDS).size() != 1
                    || cluster.controller().get(10, TimeUnit.SECONDS).id() != 0) {
                throw new AssertionError("unexpected virtual broker topology");
            }
            var quorum = admin.describeMetadataQuorum()
                    .quorumInfo()
                    .get(10, TimeUnit.SECONDS);
            if (quorum.leaderId() != 0
                    || quorum.leaderEpoch() != 0
                    || quorum.highWatermark() != 0
                    || quorum.voters().size() != 1
                    || !quorum.observers().isEmpty()) {
                throw new AssertionError("unexpected virtual metadata quorum " + quorum);
            }
            var voter = quorum.voters().get(0);
            if (voter.replicaId() != 0
                    || voter.logEndOffset() != 0
                    || voter.replicaDirectoryId().equals(
                            org.apache.kafka.common.Uuid.ZERO_UUID)
                    || voter.lastFetchTimestamp().isPresent()
                    || voter.lastCaughtUpTimestamp().isEmpty()) {
                throw new AssertionError("unexpected virtual metadata voter " + voter);
            }
            var quorumNode = quorum.nodes().get(0);
            if (quorumNode == null || quorumNode.endpoints().size() != 1) {
                throw new AssertionError("virtual metadata endpoint is missing " + quorum);
            }
            var quorumEndpoint = quorumNode.endpoints().get(0);
            if (!"CONTROLLER".equals(quorumEndpoint.listener())
                    || quorumEndpoint.host().isBlank()
                    || quorumEndpoint.port() <= 0) {
                throw new AssertionError(
                        "unexpected virtual metadata endpoint " + quorumEndpoint);
            }

            GroupListing listing = admin.listGroups(
                            new ListGroupsOptions().withTypes(Set.of(GroupType.CONSUMER)))
                    .all()
                    .get(10, TimeUnit.SECONDS)
                    .stream()
                    .filter(candidate -> candidate.groupId().equals(group))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("consumer group was not listed"));
            if (listing.type().orElse(GroupType.UNKNOWN) != GroupType.CONSUMER) {
                throw new AssertionError("unexpected consumer group type " + listing.type());
            }
            if (listing.groupState().orElse(GroupState.UNKNOWN) != GroupState.EMPTY) {
                throw new AssertionError(
                        "consumer group was not empty after close: " + listing.groupState());
            }
            admin.deleteConsumerGroups(List.of(group))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            boolean stillPresent = admin.listGroups()
                    .all()
                    .get(10, TimeUnit.SECONDS)
                    .stream()
                    .anyMatch(candidate -> candidate.groupId().equals(group));
            if (stillPresent) {
                throw new AssertionError("deleted consumer group is still listed");
            }

            String classicGroup = group + "-classic";
            TopicPartition classicPartition = new TopicPartition(topic, 0);
            admin.alterConsumerGroupOffsets(
                            classicGroup,
                            java.util.Map.of(
                                    classicPartition,
                                    new OffsetAndMetadata(
                                            0, Optional.of(0), "leader-epoch")))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            OffsetAndMetadata classicOffset = admin.listConsumerGroupOffsets(classicGroup)
                    .partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS)
                    .get(classicPartition);
            if (classicOffset == null
                    || classicOffset.offset() != 0
                    || !classicOffset.leaderEpoch().equals(Optional.of(0))
                    || !classicOffset.metadata().equals("leader-epoch")) {
                throw new AssertionError(
                        "committed leader epoch did not round-trip: " + classicOffset);
            }
            GroupListing classic = admin.listGroups(
                            new ListGroupsOptions().withTypes(Set.of(GroupType.CLASSIC)))
                    .all()
                    .get(10, TimeUnit.SECONDS)
                    .stream()
                    .filter(candidate -> candidate.groupId().equals(classicGroup))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("classic group was not listed"));
            if (classic.groupState().orElse(GroupState.UNKNOWN) != GroupState.EMPTY) {
                throw new AssertionError("offset-only classic group was not empty");
            }
            var classicDescription = admin.describeClassicGroups(List.of(classicGroup))
                    .all()
                    .get(10, TimeUnit.SECONDS)
                    .get(classicGroup);
            if (classicDescription.state() != ClassicGroupState.EMPTY
                    || !classicDescription.members().isEmpty()) {
                throw new AssertionError(
                        "unexpected classic group description " + classicDescription);
            }
            ConfigResource classicGroupConfig =
                    new ConfigResource(ConfigResource.Type.GROUP, classicGroup);
            ConfigResource configuredGroupConfig =
                    new ConfigResource(ConfigResource.Type.GROUP, group + "-configured");
            admin.incrementalAlterConfigs(Map.of(
                            configuredGroupConfig,
                            List.of(new AlterConfigOp(
                                    new ConfigEntry(
                                            "consumer.heartbeat.interval.ms", "5000"),
                                    AlterConfigOp.OpType.SET))))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            var listedGroups = admin.listConfigResources(
                            Set.of(ConfigResource.Type.GROUP),
                            new ListConfigResourcesOptions())
                    .all()
                    .get(10, TimeUnit.SECONDS);
            if (!listedGroups.contains(configuredGroupConfig)
                    || listedGroups.contains(classicGroupConfig)) {
                throw new AssertionError(
                        "GROUP resources must contain only dynamic configs "
                                + listedGroups);
            }
            TopicPartition firstPartition = new TopicPartition(topic, 0);
            ConfigResource topicConfig =
                    new ConfigResource(ConfigResource.Type.TOPIC, topic);
            var listedTopics = admin.listConfigResources(
                            Set.of(ConfigResource.Type.TOPIC),
                            new ListConfigResourcesOptions())
                    .all()
                    .get(10, TimeUnit.SECONDS);
            if (!listedTopics.contains(topicConfig)) {
                throw new AssertionError("topic config resource was not listed");
            }
            var configResources = admin.listConfigResources()
                    .all()
                    .get(10, TimeUnit.SECONDS);
            if (!configResources.contains(
                            new ConfigResource(ConfigResource.Type.BROKER, "0"))
                    || !configResources.contains(
                            new ConfigResource(ConfigResource.Type.BROKER_LOGGER, "0"))
                    || !configResources.contains(topicConfig)) {
                throw new AssertionError(
                        "default config resources were incomplete " + configResources);
            }
            admin.incrementalAlterConfigs(Map.of(
                            topicConfig,
                            List.of(new AlterConfigOp(
                                    new ConfigEntry("retention.ms", "600000"),
                                    AlterConfigOp.OpType.SET))))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            String retention = admin.describeConfigs(List.of(topicConfig))
                    .all()
                    .get(10, TimeUnit.SECONDS)
                    .get(topicConfig)
                    .get("retention.ms")
                    .value();
            if (!"600000".equals(retention)) {
                throw new AssertionError("topic config did not round-trip: " + retention);
            }

            var producerState = admin.describeProducers(List.of(firstPartition))
                    .all()
                    .get(10, TimeUnit.SECONDS)
                    .get(firstPartition);
            if (producerState.activeProducers().isEmpty()) {
                throw new AssertionError("idempotent producer state was not retained");
            }
            var activeProducer = producerState.activeProducers().get(0);
            if (activeProducer.producerId() < 0
                    || activeProducer.producerEpoch() < 0
                    || activeProducer.lastSequence() < 0
                    || activeProducer.lastTimestamp() <= 0) {
                throw new AssertionError("invalid active producer state " + activeProducer);
            }

            var logDirs = admin.describeLogDirs(List.of(0))
                    .allDescriptions()
                    .get(10, TimeUnit.SECONDS);
            var virtualDir = logDirs.getOrDefault(0, Map.of())
                    .get("/rutomq/object-store");
            if (virtualDir == null || virtualDir.error() != null) {
                throw new AssertionError("virtual object-store log dir was not described");
            }
            var replicaInfo = virtualDir.replicaInfos().get(firstPartition);
            if (replicaInfo == null
                    || replicaInfo.size() <= 0
                    || replicaInfo.offsetLag() != 0
                    || replicaInfo.isFuture()) {
                throw new AssertionError("invalid virtual replica info " + replicaInfo);
            }
            if (virtualDir.totalBytes().isPresent() || virtualDir.usableBytes().isPresent()) {
                throw new AssertionError("object-store capacity must be reported as unknown");
            }
            TopicPartitionReplica replica = new TopicPartitionReplica(topic, 0, 0);
            admin.alterReplicaLogDirs(Map.of(replica, "/rutomq/object-store"))
                    .all()
                    .get(10, TimeUnit.SECONDS);

            Optional<Throwable> electionResult = admin.electLeaders(
                            ElectionType.PREFERRED, Set.of(firstPartition))
                    .partitions()
                    .get(10, TimeUnit.SECONDS)
                    .get(firstPartition);
            if (electionResult == null
                    || electionResult.isEmpty()
                    || !(electionResult.get() instanceof ElectionNotNeededException)) {
                throw new AssertionError(
                        "virtual leader election returned " + electionResult);
            }
            admin.alterPartitionReassignments(Map.of(
                            firstPartition,
                            Optional.of(new NewPartitionReassignment(List.of(0)))))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            if (!admin.listPartitionReassignments()
                    .reassignments()
                    .get(10, TimeUnit.SECONDS)
                    .isEmpty()) {
                throw new AssertionError("virtual reassignment remained active");
            }

            admin.deleteConsumerGroupOffsets(classicGroup, Set.of(firstPartition))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            if (!admin.listConsumerGroupOffsets(classicGroup)
                    .partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS)
                    .isEmpty()) {
                throw new AssertionError("deleted classic group offset is still present");
            }

            String classicDeleteGroup = group + "-classic-delete";
            admin.alterConsumerGroupOffsets(
                            classicDeleteGroup,
                            Map.of(firstPartition, new OffsetAndMetadata(0)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            admin.deleteConsumerGroups(List.of(classicDeleteGroup))
                    .all()
                    .get(10, TimeUnit.SECONDS);

            var deleted = admin.deleteRecords(
                            Map.of(firstPartition, RecordsToDelete.beforeOffset(50)))
                    .lowWatermarks()
                    .get(firstPartition)
                    .get(10, TimeUnit.SECONDS);
            if (deleted.lowWatermark() != 50) {
                throw new AssertionError(
                        "delete records returned low watermark " + deleted.lowWatermark());
            }
            long earliest = admin.listOffsets(
                            Map.of(firstPartition, OffsetSpec.earliest()))
                    .all()
                    .get(10, TimeUnit.SECONDS)
                    .get(firstPartition)
                    .offset();
            if (earliest != 50) {
                throw new AssertionError("earliest offset after delete is " + earliest);
            }
            verifyOutOfRangeReset(bootstrap, firstPartition, 50);

            admin.createPartitions(
                            Map.of(topic, NewPartitions.increaseTo(3)),
                            new CreatePartitionsOptions().validateOnly(true))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            int validatedCount = admin.describeTopics(List.of(topic))
                    .allTopicNames()
                    .get(10, TimeUnit.SECONDS)
                    .get(topic)
                    .partitions()
                    .size();
            if (validatedCount != 2) {
                throw new AssertionError(
                        "validate-only changed partition count to " + validatedCount);
            }
            admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(3)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            int expandedCount = admin.describeTopics(List.of(topic))
                    .allTopicNames()
                    .get(10, TimeUnit.SECONDS)
                    .get(topic)
                    .partitions()
                    .size();
            if (expandedCount != 3) {
                throw new AssertionError(
                        "topic expansion returned partition count " + expandedCount);
            }
        }
    }

    private static void verifyOutOfRangeReset(
            String bootstrap, TopicPartition partition, long expectedStart) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "out-of-range-reset-" + System.nanoTime());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            consumer.seek(partition, 0);
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(250));
                if (!records.isEmpty()) {
                    long firstOffset = records.records(partition).get(0).offset();
                    if (firstOffset < expectedStart) {
                        throw new AssertionError(
                                "out-of-range reset returned offset " + firstOffset);
                    }
                    return;
                }
            }
            throw new AssertionError("out-of-range fetch did not reset to " + expectedStart);
        }
    }

    private static Properties consumerProperties(
            String bootstrap, String group, String clientId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put("group.protocol", "consumer");
        properties.put("group.remote.assignor", "uniform");
        return properties;
    }

    static Properties adminProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        return properties;
    }
}
