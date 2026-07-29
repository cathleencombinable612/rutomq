package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopicCollection;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.CreatePartitionsRequest;

public final class CreatePartitionsDuplicateSmoke {
    private static final short VERSION = 3;

    private CreatePartitionsDuplicateSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "CreatePartitionsDuplicateSmoke <bootstrap> <topic-prefix>");
        }
        String duplicate = args[1] + "-duplicate";
        String unique = args[1] + "-unique";
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(args[0]))) {
            admin.createTopics(List.of(
                            new NewTopic(duplicate, 2, (short) 1),
                            new NewTopic(unique, 1, (short) 1)))
                    .all()
                    .get(15, TimeUnit.SECONDS);

            CreatePartitionsResponseData committed = exchange(
                    args[0],
                    List.of(
                            topic(duplicate, 3),
                            topic(unique, 2),
                            topic(duplicate, 4)),
                    false,
                    54001);
            assertDuplicateResult(committed, duplicate);
            assertResult(committed, unique, Errors.NONE);
            assertPartitionCount(admin, duplicate, 2);
            assertPartitionCount(admin, unique, 2);

            CreatePartitionsResponseData validated = exchange(
                    args[0],
                    List.of(
                            topic(duplicate, 5),
                            topic(unique, 3),
                            topic(duplicate, 6)),
                    true,
                    54002);
            assertDuplicateResult(validated, duplicate);
            assertResult(validated, unique, Errors.NONE);
            assertPartitionCount(admin, duplicate, 2);
            assertPartitionCount(admin, unique, 2);
        }
        System.out.printf(
                "Kafka CreatePartitions duplicate pre-validation passed prefix=%s%n",
                args[1]);
    }

    private static CreatePartitionsResponseData exchange(
            String bootstrap,
            List<CreatePartitionsTopic> topics,
            boolean validateOnly,
            int correlationId)
            throws Exception {
        CreatePartitionsTopicCollection collection =
                new CreatePartitionsTopicCollection(topics.iterator());
        CreatePartitionsRequestData data = new CreatePartitionsRequestData()
                .setTopics(collection)
                .setTimeoutMs(15_000)
                .setValidateOnly(validateOnly);
        CreatePartitionsRequest request =
                new CreatePartitionsRequest.Builder(data).build(VERSION);
        ByteBuffer body = KafkaWire.exchange(bootstrap, request, correlationId);
        return new CreatePartitionsResponseData(new ByteBufferAccessor(body), VERSION);
    }

    private static CreatePartitionsTopic topic(String name, int count) {
        return new CreatePartitionsTopic()
                .setName(name)
                .setCount(count)
                .setAssignments(null);
    }

    private static void assertDuplicateResult(
            CreatePartitionsResponseData response, String topic) {
        long matches = response.results().stream()
                .filter(result -> result.name().equals(topic))
                .count();
        if (matches != 1) {
            throw new AssertionError(
                    "duplicate topic result count was " + matches + ": " + response);
        }
        CreatePartitionsTopicResult result = response.results().stream()
                .filter(candidate -> candidate.name().equals(topic))
                .findFirst()
                .orElseThrow();
        if (result.errorCode() != Errors.INVALID_REQUEST.code()
                || !"Duplicate topic name.".equals(result.errorMessage())) {
            throw new AssertionError("unexpected duplicate result: " + result);
        }
    }

    private static void assertResult(
            CreatePartitionsResponseData response, String topic, Errors expected) {
        CreatePartitionsTopicResult result = response.results().stream()
                .filter(candidate -> candidate.name().equals(topic))
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing result for " + topic));
        if (result.errorCode() != expected.code()) {
            throw new AssertionError("unexpected result for " + topic + ": " + result);
        }
    }

    private static void assertPartitionCount(Admin admin, String topic, int expected)
            throws Exception {
        int actual = admin.describeTopics(List.of(topic))
                .allTopicNames()
                .get(15, TimeUnit.SECONDS)
                .get(topic)
                .partitions()
                .size();
        if (actual != expected) {
            throw new AssertionError(
                    "partition count for " + topic + " was " + actual + ", expected " + expected);
        }
    }
}
