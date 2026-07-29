package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.config.ConfigResource;

public final class StreamsTopologyValidationSmoke {
    private static final short VERSION = 0;

    private StreamsTopologyValidationSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "StreamsTopologyValidationSmoke <bootstrap> <group-prefix>");
        }

        StreamsGroupHeartbeatRequestData.Subtopology internal =
                subtopology(List.of("__consumer_offsets"))
                        .setRepartitionSinkTopics(List.of("__transaction_state"))
                        .setRepartitionSourceTopics(List.of(
                                topic("__share_group_state")));
        assertInvalid(
                exchange(args[0], request(args[1] + "-internal", internal), 74001),
                "Use of Kafka internal topics __consumer_offsets,__transaction_state,"
                        + "__share_group_state in a Kafka Streams topology is prohibited.");

        StreamsGroupHeartbeatRequestData.Subtopology invalid =
                subtopology(List.of("a "))
                        .setRepartitionSinkTopics(List.of("b?"))
                        .setRepartitionSourceTopics(List.of(topic("c!")))
                        .setStateChangelogTopics(List.of(topic("d/")));
        assertInvalid(
                exchange(args[0], request(args[1] + "-invalid", invalid), 74002),
                "Topic names a ,b?,c!,d/ are not valid topic names.");

        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]))) {
            String source = args[1] + "-source";
            String defaultTopic = args[1] + "-default-changelog";
            String replicaTopic = args[1] + "-rf2-changelog";
            String unsupportedConfigTopic = args[1] + "-config-changelog";
            admin.createTopics(List.of(new NewTopic(source, 1, (short) 1)))
                    .all()
                    .get(15, TimeUnit.SECONDS);

            StreamsGroupHeartbeatRequestData.Subtopology withDefault =
                    subtopology(List.of(source))
                            .setStateChangelogTopics(List.of(internalTopic(
                                    defaultTopic,
                                    (short) 0,
                                    Map.of(
                                            "cleanup.policy", "compact",
                                            "segment.bytes", "52428800"))));
            StreamsGroupHeartbeatResponseData created = exchange(
                    args[0],
                    request(args[1] + "-default", withDefault),
                    74003);
            created = awaitReady(
                    args[0], args[1] + "-default", created, 74006);
            assertReady(created);
            int replicas = admin.describeTopics(List.of(defaultTopic))
                    .allTopicNames()
                    .get(15, TimeUnit.SECONDS)
                    .get(defaultTopic)
                    .partitions()
                    .get(0)
                    .replicas()
                    .size();
            ConfigResource resource =
                    new ConfigResource(ConfigResource.Type.TOPIC, defaultTopic);
            ConfigEntry cleanup = admin.describeConfigs(List.of(resource))
                    .all()
                    .get(15, TimeUnit.SECONDS)
                    .get(resource)
                    .get("cleanup.policy");
            if (replicas != 1
                    || cleanup == null
                    || !"compact".equals(cleanup.value())
                    || cleanup.source()
                            != ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
                throw new AssertionError(
                        "unexpected Streams internal topic defaults/config " + cleanup);
            }

            StreamsGroupHeartbeatRequestData.Subtopology withReplicaTwo =
                    subtopology(List.of(source))
                            .setStateChangelogTopics(List.of(internalTopic(
                                    replicaTopic, (short) 2, Map.of())));
            assertMissingInternal(
                    exchange(
                            args[0],
                            request(args[1] + "-rf2", withReplicaTwo),
                            74004),
                    "replication factor exceeds the one-broker virtual topology");

            StreamsGroupHeartbeatRequestData.Subtopology withUnsupportedConfig =
                    subtopology(List.of(source))
                            .setStateChangelogTopics(List.of(internalTopic(
                                    unsupportedConfigTopic,
                                    (short) 0,
                                    Map.of("remote.storage.enable", "true"))));
            assertMissingInternal(
                    exchange(
                            args[0],
                            request(args[1] + "-config", withUnsupportedConfig),
                            74005),
                    "topic configuration remote.storage.enable is unsupported");

            var names = admin.listTopics().names().get(15, TimeUnit.SECONDS);
            if (names.contains(replicaTopic) || names.contains(unsupportedConfigTopic)) {
                throw new AssertionError(
                        "failed Streams internal topic creation mutated metadata " + names);
            }
        }

        System.out.printf(
                "Kafka Streams topology and internal-topic creation validation passed "
                        + "prefix=%s%n",
                args[1]);
    }

    private static StreamsGroupHeartbeatRequestData request(
            String group,
            StreamsGroupHeartbeatRequestData.Subtopology subtopology) {
        return new StreamsGroupHeartbeatRequestData()
                .setGroupId(group)
                .setMemberId("member-a")
                .setMemberEpoch(0)
                .setEndpointInformationEpoch(-1)
                .setRebalanceTimeoutMs(300_000)
                .setTopology(new StreamsGroupHeartbeatRequestData.Topology()
                        .setEpoch(0)
                        .setSubtopologies(List.of(subtopology)))
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setProcessId("process-a")
                .setClientTags(List.of());
    }

    private static StreamsGroupHeartbeatRequestData.Subtopology subtopology(
            List<String> sources) {
        return new StreamsGroupHeartbeatRequestData.Subtopology()
                .setSubtopologyId("0")
                .setSourceTopics(sources)
                .setSourceTopicRegex(List.of())
                .setRepartitionSinkTopics(List.of())
                .setRepartitionSourceTopics(List.of())
                .setStateChangelogTopics(List.of())
                .setCopartitionGroups(List.of());
    }

    private static StreamsGroupHeartbeatRequestData.TopicInfo topic(String name) {
        return new StreamsGroupHeartbeatRequestData.TopicInfo().setName(name);
    }

    private static StreamsGroupHeartbeatRequestData.TopicInfo internalTopic(
            String name,
            short replicationFactor,
            Map<String, String> configs) {
        StreamsGroupHeartbeatRequestData.TopicInfo topic =
                new StreamsGroupHeartbeatRequestData.TopicInfo()
                        .setName(name)
                        .setPartitions(0)
                        .setReplicationFactor(replicationFactor);
        if (!configs.isEmpty()) {
            topic.setTopicConfigs(configs.entrySet().stream()
                    .map(entry -> new StreamsGroupHeartbeatRequestData.KeyValue()
                            .setKey(entry.getKey())
                            .setValue(entry.getValue()))
                    .toList());
        }
        return topic;
    }

    private static StreamsGroupHeartbeatResponseData exchange(
            String bootstrap,
            StreamsGroupHeartbeatRequestData request,
            int correlationId)
            throws Exception {
        ByteBuffer body = KafkaWire.exchangeRaw(
                bootstrap,
                ApiKeys.STREAMS_GROUP_HEARTBEAT,
                VERSION,
                request,
                correlationId);
        return new StreamsGroupHeartbeatResponseData(
                new ByteBufferAccessor(body), VERSION);
    }

    private static StreamsGroupHeartbeatResponseData awaitReady(
            String bootstrap,
            String group,
            StreamsGroupHeartbeatResponseData response,
            int correlationId)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (response.errorCode() == Errors.NONE.code()
                && response.status() != null
                && response.status().size() == 1
                && response.status().get(0).statusCode() == 5
                && System.nanoTime() < deadline) {
            Thread.sleep(250);
            StreamsGroupHeartbeatRequestData heartbeat =
                    new StreamsGroupHeartbeatRequestData()
                            .setGroupId(group)
                            .setMemberId(response.memberId())
                            .setMemberEpoch(response.memberEpoch())
                            .setEndpointInformationEpoch(-1)
                            .setRebalanceTimeoutMs(-1)
                            .setTopology(null)
                            .setActiveTasks(null)
                            .setStandbyTasks(null)
                            .setWarmupTasks(null);
            response = exchange(bootstrap, heartbeat, correlationId++);
        }
        return response;
    }

    private static void assertInvalid(
            StreamsGroupHeartbeatResponseData response, String message) {
        if (response.errorCode() != Errors.STREAMS_INVALID_TOPOLOGY.code()
                || !message.equals(response.errorMessage())
                || !response.memberId().isEmpty()
                || response.heartbeatIntervalMs() != 0) {
            throw new AssertionError(
                    "unexpected streams topology validation response " + response);
        }
    }

    private static void assertReady(StreamsGroupHeartbeatResponseData response) {
        if (response.errorCode() != Errors.NONE.code()
                || response.status() == null
                || !response.status().isEmpty()
                || response.activeTasks() == null
                || response.activeTasks().isEmpty()) {
            throw new AssertionError(
                    "unexpected ready Streams topology response " + response);
        }
    }

    private static void assertMissingInternal(
            StreamsGroupHeartbeatResponseData response, String detailFragment) {
        var missing = response.status() == null
                ? List.<StreamsGroupHeartbeatResponseData.Status>of()
                : response.status().stream()
                        .filter(status -> status.statusCode() == 3)
                        .toList();
        if (response.errorCode() != Errors.NONE.code()
                || response.status() == null
                || missing.size() != 1
                || response.status().stream()
                        .anyMatch(status ->
                                status.statusCode() != 3
                                        && status.statusCode() != 5)
                || !missing.get(0).statusDetail().contains(detailFragment)
                || response.activeTasks() == null
                || !response.activeTasks().isEmpty()) {
            throw new AssertionError(
                    "unexpected missing internal topic response " + response);
        }
    }
}
