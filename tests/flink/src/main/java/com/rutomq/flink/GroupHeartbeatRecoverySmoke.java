package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

public final class GroupHeartbeatRecoverySmoke {
    private static final short CONSUMER_VERSION = 1;
    private static final short OFFSET_COMMIT_VERSION = 9;
    private static final short SHARE_VERSION = 1;
    private static final short STREAMS_VERSION = 0;

    private GroupHeartbeatRecoverySmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "GroupHeartbeatRecoverySmoke <bootstrap> <prefix>");
        }
        String topic = args[1] + "-topic";
        try (Admin admin =
                Admin.create(ConsumerProtocolSmoke.adminProperties(args[0]))) {
            admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1)))
                    .all()
                    .get(15, TimeUnit.SECONDS);
        }

        consumerRecovery(args[0], args[1] + "-consumer", topic);
        consumerStaticLeave(args[0], args[1] + "-static", topic);
        shareRecovery(args[0], args[1] + "-share", topic);
        streamsRecovery(args[0], args[1] + "-streams", topic);
        System.out.printf(
                "Kafka consumer assignment epochs and consumer/share/streams heartbeat recovery passed prefix=%s%n",
                args[1]);
    }

    private static void consumerRecovery(
            String bootstrap, String group, String topic) throws Exception {
        ConsumerGroupHeartbeatResponseData first =
                consumer(
                        bootstrap,
                        consumerJoin(group, "consumer-a", topic, null),
                        96301);
        assertOk(first.errorCode(), "consumer first join", first);
        ConsumerGroupHeartbeatResponseData assigned =
                awaitConsumerAssignment(
                        bootstrap,
                        group,
                        "consumer-a",
                        first,
                        96400);
        consumer(
                bootstrap,
                consumerRegular(group, "consumer-a", assigned.memberEpoch())
                        .setTopicPartitions(consumerOwned(assigned)),
                96302);
        ConsumerGroupHeartbeatResponseData second =
                consumer(
                        bootstrap,
                        consumerJoin(group, "consumer-b", topic, null),
                        96303);
        assertOk(second.errorCode(), "consumer second join", second);
        Thread.sleep(1_200);

        ConsumerGroupHeartbeatResponseData revoking = null;
        for (int attempt = 0; attempt < 100; attempt++) {
            ConsumerGroupHeartbeatResponseData candidate =
                    consumer(
                            bootstrap,
                            consumerRegular(
                                    group,
                                    "consumer-a",
                                    assigned.memberEpoch()),
                            96500 + attempt);
            assertOk(
                    candidate.errorCode(),
                    "consumer revocation",
                    candidate);
            int partitions =
                    consumerOwned(candidate).stream()
                            .mapToInt(
                                    assignment ->
                                            assignment.partitions().size())
                            .sum();
            if (partitions == 1) {
                revoking = candidate;
                break;
            }
            Thread.sleep(25);
        }
        if (revoking == null) {
            throw new AssertionError(
                    "consumer offloaded revocation was not published");
        }
        ConsumerGroupHeartbeatRequestData lostResponse =
                consumerRegular(
                                group,
                                "consumer-a",
                                assigned.memberEpoch())
                        .setRebalanceTimeoutMs(300_000)
                        .setSubscribedTopicNames(List.of(topic))
                        .setTopicPartitions(consumerOwned(revoking));
        ConsumerGroupHeartbeatResponseData advanced =
                consumer(bootstrap, lostResponse, 96305);
        assertEpoch(
                advanced,
                assigned.memberEpoch() + 1,
                "consumer epoch advance");
        ConsumerGroupHeartbeatResponseData recovered =
                consumer(bootstrap, lostResponse, 96306);
        assertEpoch(
                recovered,
                advanced.memberEpoch(),
                "consumer previous-epoch retry");

        int retainedPartition =
                consumerOwned(revoking).get(0).partitions().get(0);
        int movedPartition = retainedPartition == 0 ? 1 : 0;
        OffsetCommitResponseData commit =
                offsetCommit(
                        bootstrap,
                        group,
                        "consumer-a",
                        assigned.memberEpoch(),
                        topic,
                        List.of(
                                new OffsetCommitRequestData
                                                .OffsetCommitRequestPartition()
                                        .setPartitionIndex(retainedPartition)
                                        .setCommittedOffset(10),
                                new OffsetCommitRequestData
                                                .OffsetCommitRequestPartition()
                                        .setPartitionIndex(movedPartition)
                                        .setCommittedOffset(11)),
                        96307);
        short retainedError =
                commit.topics().get(0).partitions().stream()
                        .filter(
                                partition ->
                                        partition.partitionIndex()
                                                == retainedPartition)
                        .findFirst()
                        .orElseThrow()
                        .errorCode();
        short movedError =
                commit.topics().get(0).partitions().stream()
                        .filter(
                                partition ->
                                        partition.partitionIndex()
                                                == movedPartition)
                        .findFirst()
                        .orElseThrow()
                        .errorCode();
        if (retainedError != Errors.NONE.code()
                || movedError != Errors.STALE_MEMBER_EPOCH.code()) {
            throw new AssertionError(
                    "consumer assignment epoch validation mismatch: " + commit);
        }

        ConsumerGroupHeartbeatResponseData fenced =
                consumer(
                        bootstrap,
                        consumerRegular(
                                        group,
                                        "consumer-a",
                                        assigned.memberEpoch())
                                .setTopicPartitions(
                                        consumerOwned(assigned)),
                        96308);
        if (fenced.errorCode() != Errors.FENCED_MEMBER_EPOCH.code()) {
            throw new AssertionError(
                    "consumer over-owned previous epoch was accepted: "
                            + fenced);
        }
    }

    private static void consumerStaticLeave(
            String bootstrap, String group, String topic) throws Exception {
        String instanceId = "instance-a";
        ConsumerGroupHeartbeatResponseData joined =
                consumer(
                        bootstrap,
                        consumerJoin(group, "static-a", topic, instanceId),
                        96311);
        assertOk(joined.errorCode(), "static consumer join", joined);
        ConsumerGroupHeartbeatResponseData assigned =
                awaitConsumerAssignment(
                        bootstrap,
                        group,
                        "static-a",
                        joined,
                        96900);
        consumer(
                bootstrap,
                consumerRegular(group, "static-a", assigned.memberEpoch())
                        .setInstanceId(instanceId)
                        .setTopicPartitions(consumerOwned(assigned)),
                96312);
        ConsumerGroupHeartbeatResponseData left =
                consumer(
                        bootstrap,
                        consumerRegular(group, "static-a", -2)
                                .setInstanceId(instanceId),
                        96313);
        assertEpoch(left, -2, "static consumer temporary leave");
        if (left.assignment() != null || left.heartbeatIntervalMs() != 0) {
            throw new AssertionError(
                    "static leave returned heartbeat state: " + left);
        }

        ConsumerGroupHeartbeatResponseData rejoined =
                consumer(
                        bootstrap,
                        consumerJoin(group, "static-b", topic, instanceId),
                        96314);
        assertEpoch(
                rejoined,
                assigned.memberEpoch(),
                "static consumer reconnect");
        if (consumerOwned(rejoined).isEmpty()) {
            throw new AssertionError(
                    "static consumer lost its assignment: " + rejoined);
        }
    }

    private static void shareRecovery(
            String bootstrap, String group, String topic) throws Exception {
        ShareGroupHeartbeatResponseData first =
                share(bootstrap, shareJoin(group, "share-a", topic), 96321);
        assertOk(first.errorCode(), "share first join", first);
        ShareGroupHeartbeatResponseData assigned =
                awaitShareAssignment(
                        bootstrap, group, "share-a", first, 97000);
        share(
                bootstrap,
                shareRegular(group, "share-a", assigned.memberEpoch()),
                96320);
        ShareGroupHeartbeatResponseData second =
                share(bootstrap, shareJoin(group, "share-b", topic), 96322);
        assertEpoch(
                second,
                assigned.memberEpoch(),
                "share delayed second join");
        Thread.sleep(1_200);
        ShareGroupHeartbeatRequestData lostResponse =
                shareRegular(group, "share-a", assigned.memberEpoch());
        ShareGroupHeartbeatResponseData advanced = null;
        for (int attempt = 0; attempt < 100; attempt++) {
            ShareGroupHeartbeatResponseData candidate =
                    share(bootstrap, lostResponse, 96600 + attempt);
            assertOk(
                    candidate.errorCode(),
                    "share assignment publication",
                    candidate);
            if (candidate.memberEpoch() > assigned.memberEpoch()) {
                advanced = candidate;
                break;
            }
            Thread.sleep(25);
        }
        if (advanced == null) {
            throw new AssertionError(
                    "share offloaded assignment was not published");
        }
        assertEpoch(
                share(bootstrap, lostResponse, 96324),
                advanced.memberEpoch(),
                "share duplicate previous-epoch retry");

        share(bootstrap, shareJoin(group, "share-c", topic), 96325);
        Thread.sleep(1_200);
        ShareGroupHeartbeatResponseData next = null;
        for (int attempt = 0; attempt < 100; attempt++) {
            ShareGroupHeartbeatResponseData candidate =
                    share(
                            bootstrap,
                            shareRegular(
                                    group,
                                    "share-a",
                                    advanced.memberEpoch()),
                            96700 + attempt);
            assertOk(
                    candidate.errorCode(),
                    "share next assignment",
                    candidate);
            if (candidate.memberEpoch() > advanced.memberEpoch()) {
                next = candidate;
                break;
            }
            Thread.sleep(25);
        }
        if (next == null) {
            throw new AssertionError(
                    "share next offloaded assignment was not published");
        }
        assertEpoch(
                next,
                advanced.memberEpoch() + 1,
                "share next assignment");
        ShareGroupHeartbeatResponseData fenced =
                share(bootstrap, lostResponse, 96327);
        if (fenced.errorCode() != Errors.FENCED_MEMBER_EPOCH.code()) {
            throw new AssertionError(
                    "share stale epoch was accepted: " + fenced);
        }
    }

    private static void streamsRecovery(
            String bootstrap, String group, String topic) throws Exception {
        StreamsGroupHeartbeatResponseData first =
                streams(
                        bootstrap,
                        streamsJoin(group, "streams-a", topic),
                        96331);
        assertOk(first.errorCode(), "streams first join", first);
        StreamsGroupHeartbeatResponseData assigned =
                awaitStreamsAssignment(
                        bootstrap,
                        group,
                        "streams-a",
                        first,
                        97100);
        StreamsGroupHeartbeatResponseData second =
                streams(
                        bootstrap,
                        streamsJoin(group, "streams-b", topic),
                        96332);
        assertOk(second.errorCode(), "streams second join", second);

        Thread.sleep(4_000);
        StreamsGroupHeartbeatRequestData lostResponse =
                streamsRegular(group, "streams-a", assigned.memberEpoch())
                        .setActiveTasks(List.of())
                        .setStandbyTasks(List.of())
                        .setWarmupTasks(List.of());
        StreamsGroupHeartbeatResponseData advanced = null;
        for (int attempt = 0; attempt < 100; attempt++) {
            StreamsGroupHeartbeatResponseData candidate =
                    streams(bootstrap, lostResponse, 96800 + attempt);
            assertOk(
                    candidate.errorCode(),
                    "streams epoch advance",
                    candidate);
            if (candidate.memberEpoch() > assigned.memberEpoch()
                    && taskCount(candidate.activeTasks()) == 1) {
                advanced = candidate;
                break;
            }
            Thread.sleep(25);
        }
        if (advanced == null) {
            throw new AssertionError(
                    "streams offloaded assignment was not published");
        }
        assertEpoch(
                streams(bootstrap, lostResponse, 96334),
                advanced.memberEpoch(),
                "streams previous-epoch retry");

        StreamsGroupHeartbeatResponseData fenced =
                streams(
                        bootstrap,
                        streamsRegular(
                                        group,
                                        "streams-a",
                                        assigned.memberEpoch())
                                .setActiveTasks(
                                        List.of(
                                                new StreamsGroupHeartbeatRequestData
                                                                .TaskIds()
                                                        .setSubtopologyId("0")
                                                        .setPartitions(
                                                                List.of(0, 1))))
                                .setStandbyTasks(List.of())
                                .setWarmupTasks(List.of()),
                        96335);
        if (fenced.errorCode() != Errors.FENCED_MEMBER_EPOCH.code()) {
            throw new AssertionError(
                    "streams over-owned previous epoch was accepted: "
                            + fenced);
        }
    }

    private static ConsumerGroupHeartbeatRequestData consumerJoin(
            String group, String member, String topic, String instanceId) {
        return consumerRegular(group, member, 0)
                .setInstanceId(instanceId)
                .setRebalanceTimeoutMs(300_000)
                .setSubscribedTopicNames(List.of(topic))
                .setTopicPartitions(List.of());
    }

    private static ConsumerGroupHeartbeatRequestData consumerRegular(
            String group, String member, int epoch) {
        return new ConsumerGroupHeartbeatRequestData()
                .setGroupId(group)
                .setMemberId(member)
                .setMemberEpoch(epoch)
                .setRebalanceTimeoutMs(-1)
                .setSubscribedTopicNames(null)
                .setTopicPartitions(null);
    }

    private static List<ConsumerGroupHeartbeatRequestData.TopicPartitions>
            consumerOwned(ConsumerGroupHeartbeatResponseData response) {
        if (response.assignment() == null) {
            return List.of();
        }
        return response.assignment().topicPartitions().stream()
                .map(topic ->
                        new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                                .setTopicId(topic.topicId())
                                .setPartitions(topic.partitions()))
                .toList();
    }

    private static ConsumerGroupHeartbeatResponseData
            awaitConsumerAssignment(
                    String bootstrap,
                    String group,
                    String member,
                    ConsumerGroupHeartbeatResponseData first,
                    int correlationBase)
                    throws Exception {
        ConsumerGroupHeartbeatResponseData assigned = first;
        for (int attempt = 0;
                attempt < 100
                        && assigned.memberEpoch() == first.memberEpoch();
                attempt++) {
            assigned =
                    consumer(
                            bootstrap,
                            consumerRegular(
                                            group,
                                            member,
                                            assigned.memberEpoch())
                                    .setTopicPartitions(
                                            consumerOwned(assigned)),
                            correlationBase + attempt);
            assertOk(
                    assigned.errorCode(),
                    "consumer initial assignment",
                    assigned);
            if (assigned.memberEpoch() == first.memberEpoch()) {
                Thread.sleep(25);
            }
        }
        if (assigned.memberEpoch() <= first.memberEpoch()
                || consumerOwned(assigned).isEmpty()) {
            throw new AssertionError(
                    "consumer offloaded assignment was not published: "
                            + assigned);
        }
        return assigned;
    }

    private static ShareGroupHeartbeatRequestData shareJoin(
            String group, String member, String topic) {
        return shareRegular(group, member, 0)
                .setSubscribedTopicNames(List.of(topic));
    }

    private static ShareGroupHeartbeatRequestData shareRegular(
            String group, String member, int epoch) {
        return new ShareGroupHeartbeatRequestData()
                .setGroupId(group)
                .setMemberId(member)
                .setMemberEpoch(epoch)
                .setSubscribedTopicNames(null);
    }

    private static ShareGroupHeartbeatResponseData awaitShareAssignment(
            String bootstrap,
            String group,
            String member,
            ShareGroupHeartbeatResponseData first,
            int correlationBase)
            throws Exception {
        ShareGroupHeartbeatResponseData assigned = first;
        for (int attempt = 0;
                attempt < 100
                        && assigned.memberEpoch() == first.memberEpoch();
                attempt++) {
            assigned =
                    share(
                            bootstrap,
                            shareRegular(
                                    group,
                                    member,
                                    assigned.memberEpoch()),
                            correlationBase + attempt);
            assertOk(
                    assigned.errorCode(),
                    "share initial assignment",
                    assigned);
            if (assigned.memberEpoch() == first.memberEpoch()) {
                Thread.sleep(25);
            }
        }
        if (assigned.memberEpoch() <= first.memberEpoch()
                || assigned.assignment() == null
                || assigned.assignment().topicPartitions().isEmpty()) {
            throw new AssertionError(
                    "share offloaded assignment was not published: "
                            + assigned);
        }
        return assigned;
    }

    private static StreamsGroupHeartbeatRequestData streamsJoin(
            String group, String member, String topic) {
        StreamsGroupHeartbeatRequestData.Subtopology subtopology =
                new StreamsGroupHeartbeatRequestData.Subtopology()
                        .setSubtopologyId("0")
                        .setSourceTopics(List.of(topic))
                        .setSourceTopicRegex(List.of())
                        .setStateChangelogTopics(List.of())
                        .setRepartitionSinkTopics(List.of())
                        .setRepartitionSourceTopics(List.of())
                        .setCopartitionGroups(List.of());
        return streamsRegular(group, member, 0)
                .setRebalanceTimeoutMs(300_000)
                .setTopology(
                        new StreamsGroupHeartbeatRequestData.Topology()
                                .setEpoch(0)
                                .setSubtopologies(List.of(subtopology)))
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of())
                .setProcessId("process-" + member)
                .setClientTags(List.of());
    }

    private static StreamsGroupHeartbeatRequestData streamsRegular(
            String group, String member, int epoch) {
        return new StreamsGroupHeartbeatRequestData()
                .setGroupId(group)
                .setMemberId(member)
                .setMemberEpoch(epoch)
                .setEndpointInformationEpoch(-1)
                .setRebalanceTimeoutMs(-1)
                .setTopology(null)
                .setActiveTasks(null)
                .setStandbyTasks(null)
                .setWarmupTasks(null);
    }

    private static List<StreamsGroupHeartbeatRequestData.TaskIds> streamsTasks(
            Integer... partitions) {
        return List.of(
                new StreamsGroupHeartbeatRequestData.TaskIds()
                        .setSubtopologyId("0")
                        .setPartitions(List.of(partitions)));
    }

    private static int taskCount(
            List<StreamsGroupHeartbeatResponseData.TaskIds> tasks) {
        if (tasks == null) {
            return 0;
        }
        return tasks.stream().mapToInt(task -> task.partitions().size()).sum();
    }

    private static StreamsGroupHeartbeatResponseData
            awaitStreamsAssignment(
                    String bootstrap,
                    String group,
                    String member,
                    StreamsGroupHeartbeatResponseData first,
                    int correlationBase)
                    throws Exception {
        StreamsGroupHeartbeatResponseData assigned = first;
        for (int attempt = 0;
                attempt < 100
                        && assigned.memberEpoch() == first.memberEpoch();
                attempt++) {
            assigned =
                    streams(
                            bootstrap,
                            streamsRegular(
                                            group,
                                            member,
                                            assigned.memberEpoch())
                                    .setActiveTasks(List.of())
                                    .setStandbyTasks(List.of())
                                    .setWarmupTasks(List.of()),
                            correlationBase + attempt);
            assertOk(
                    assigned.errorCode(),
                    "streams initial assignment",
                    assigned);
            if (assigned.memberEpoch() == first.memberEpoch()) {
                Thread.sleep(25);
            }
        }
        if (assigned.memberEpoch() <= first.memberEpoch()
                || taskCount(assigned.activeTasks()) == 0) {
            throw new AssertionError(
                    "streams offloaded assignment was not published: "
                            + assigned);
        }
        return assigned;
    }

    private static ConsumerGroupHeartbeatResponseData consumer(
            String bootstrap,
            ConsumerGroupHeartbeatRequestData request,
            int correlationId)
            throws Exception {
        ByteBuffer body =
                KafkaWire.exchangeRaw(
                        bootstrap,
                        ApiKeys.CONSUMER_GROUP_HEARTBEAT,
                        CONSUMER_VERSION,
                        request,
                        correlationId);
        return new ConsumerGroupHeartbeatResponseData(
                new ByteBufferAccessor(body), CONSUMER_VERSION);
    }

    private static OffsetCommitResponseData offsetCommit(
            String bootstrap,
            String group,
            String member,
            int epoch,
            String topic,
            List<OffsetCommitRequestData.OffsetCommitRequestPartition>
                    partitions,
            int correlationId)
            throws Exception {
        OffsetCommitRequestData request =
                new OffsetCommitRequestData()
                        .setGroupId(group)
                        .setGenerationIdOrMemberEpoch(epoch)
                        .setMemberId(member)
                        .setTopics(
                                List.of(
                                        new OffsetCommitRequestData
                                                        .OffsetCommitRequestTopic()
                                                .setName(topic)
                                                .setPartitions(partitions)));
        ByteBuffer body =
                KafkaWire.exchangeRaw(
                        bootstrap,
                        ApiKeys.OFFSET_COMMIT,
                        OFFSET_COMMIT_VERSION,
                        request,
                        correlationId);
        return new OffsetCommitResponseData(
                new ByteBufferAccessor(body), OFFSET_COMMIT_VERSION);
    }

    private static ShareGroupHeartbeatResponseData share(
            String bootstrap,
            ShareGroupHeartbeatRequestData request,
            int correlationId)
            throws Exception {
        ByteBuffer body =
                KafkaWire.exchangeRaw(
                        bootstrap,
                        ApiKeys.SHARE_GROUP_HEARTBEAT,
                        SHARE_VERSION,
                        request,
                        correlationId);
        return new ShareGroupHeartbeatResponseData(
                new ByteBufferAccessor(body), SHARE_VERSION);
    }

    private static StreamsGroupHeartbeatResponseData streams(
            String bootstrap,
            StreamsGroupHeartbeatRequestData request,
            int correlationId)
            throws Exception {
        ByteBuffer body =
                KafkaWire.exchangeRaw(
                        bootstrap,
                        ApiKeys.STREAMS_GROUP_HEARTBEAT,
                        STREAMS_VERSION,
                        request,
                        correlationId);
        return new StreamsGroupHeartbeatResponseData(
                new ByteBufferAccessor(body), STREAMS_VERSION);
    }

    private static void assertOk(
            short errorCode, String operation, Object response) {
        if (errorCode != Errors.NONE.code()) {
            throw new AssertionError(operation + " failed: " + response);
        }
    }

    private static void assertEpoch(
            ConsumerGroupHeartbeatResponseData response,
            int expected,
            String operation) {
        assertOk(response.errorCode(), operation, response);
        if (response.memberEpoch() != expected) {
            throw new AssertionError(
                    operation + " expected epoch " + expected + ": " + response);
        }
    }

    private static void assertEpoch(
            ShareGroupHeartbeatResponseData response,
            int expected,
            String operation) {
        assertOk(response.errorCode(), operation, response);
        if (response.memberEpoch() != expected) {
            throw new AssertionError(
                    operation + " expected epoch " + expected + ": " + response);
        }
    }

    private static void assertEpoch(
            StreamsGroupHeartbeatResponseData response,
            int expected,
            String operation) {
        assertOk(response.errorCode(), operation, response);
        if (response.memberEpoch() != expected) {
            throw new AssertionError(
                    operation + " expected epoch " + expected + ": " + response);
        }
    }
}
