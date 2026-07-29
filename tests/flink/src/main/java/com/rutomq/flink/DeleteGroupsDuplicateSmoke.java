package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DeleteGroupsRequest;

public final class DeleteGroupsDuplicateSmoke {
    private static final short VERSION = 2;

    private DeleteGroupsDuplicateSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                    "DeleteGroupsDuplicateSmoke <bootstrap> <topic> <group-prefix>");
        }
        String duplicate = args[2] + "-duplicate";
        String unique = args[2] + "-unique";
        ConsumerProtocolSmoke.createTopic(args[0], args[1]);
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(args[0]))) {
            TopicPartition partition = new TopicPartition(args[1], 0);
            for (String group : List.of(duplicate, unique)) {
                admin.alterConsumerGroupOffsets(
                                group,
                                Map.of(partition, new OffsetAndMetadata(0)))
                        .all()
                        .get(15, TimeUnit.SECONDS);
            }

            DeleteGroupsRequestData data = new DeleteGroupsRequestData()
                    .setGroupsNames(List.of(duplicate, unique, duplicate));
            DeleteGroupsRequest request =
                    new DeleteGroupsRequest.Builder(data).build(VERSION);
            ByteBuffer body = KafkaWire.exchange(args[0], request, 73001);
            DeleteGroupsResponseData response =
                    new DeleteGroupsResponseData(
                            new ByteBufferAccessor(body), VERSION);

            if (response.results().size() != 2) {
                throw new AssertionError(
                        "DeleteGroups returned one result per request entry instead of"
                                + " per distinct group: "
                                + response);
            }
            assertDeleted(response, duplicate);
            assertDeleted(response, unique);
        }
        System.out.printf(
                "Kafka DeleteGroups distinct-name semantics passed prefix=%s%n",
                args[2]);
    }

    private static void assertDeleted(
            DeleteGroupsResponseData response, String group) {
        long matches = response.results().stream()
                .filter(result -> group.equals(result.groupId()))
                .count();
        if (matches != 1) {
            throw new AssertionError(
                    "DeleteGroups result count for " + group + " was " + matches);
        }
        short error = response.results().stream()
                .filter(result -> group.equals(result.groupId()))
                .findFirst()
                .orElseThrow()
                .errorCode();
        if (error != Errors.NONE.code()) {
            throw new AssertionError(
                    "DeleteGroups failed for " + group + " with " + error);
        }
    }
}
