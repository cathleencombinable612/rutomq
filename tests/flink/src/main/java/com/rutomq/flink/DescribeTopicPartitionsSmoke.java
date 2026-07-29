package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData.Cursor;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData.TopicRequest;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

public final class DescribeTopicPartitionsSmoke {
    private static final short VERSION = 0;

    private DescribeTopicPartitionsSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "DescribeTopicPartitionsSmoke <bootstrap> <topic-prefix>");
        }
        String bootstrap = args[0];
        String topic = args[1] + "-bounded";
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(bootstrap))) {
            admin.createTopics(List.of(new NewTopic(topic, 3, (short) 1)))
                    .all()
                    .get(15, TimeUnit.SECONDS);
        }

        assertPage(exchange(
                        bootstrap,
                        request(topic, 0, null),
                        75001),
                topic,
                1,
                1);
        assertPage(exchange(
                        bootstrap,
                        request(topic, Integer.MAX_VALUE, null),
                        75002),
                topic,
                2,
                2);
        assertPage(exchange(
                        bootstrap,
                        request(
                                topic,
                                Integer.MAX_VALUE,
                                new Cursor()
                                        .setTopicName(topic)
                                        .setPartitionIndex(2)),
                        75003),
                topic,
                1,
                null);

        assertInvalid(exchange(
                bootstrap,
                request(
                        topic,
                        10,
                        new Cursor()
                                .setTopicName(args[1] + "-absent")
                                .setPartitionIndex(0)),
                75004));
        assertInvalid(exchange(
                bootstrap,
                request(
                        topic,
                        10,
                        new Cursor()
                                .setTopicName(topic)
                                .setPartitionIndex(-1)),
                75005));

        System.out.printf(
                "Kafka DescribeTopicPartitions cursor and page bounds passed topic=%s%n",
                topic);
    }

    private static DescribeTopicPartitionsRequestData request(
            String topic, int limit, Cursor cursor) {
        return new DescribeTopicPartitionsRequestData()
                .setTopics(List.of(new TopicRequest().setName(topic)))
                .setResponsePartitionLimit(limit)
                .setCursor(cursor);
    }

    private static DescribeTopicPartitionsResponseData exchange(
            String bootstrap,
            DescribeTopicPartitionsRequestData request,
            int correlationId)
            throws Exception {
        ByteBuffer body = KafkaWire.exchangeRaw(
                bootstrap,
                ApiKeys.DESCRIBE_TOPIC_PARTITIONS,
                VERSION,
                request,
                correlationId);
        return new DescribeTopicPartitionsResponseData(
                new ByteBufferAccessor(body), VERSION);
    }

    private static void assertPage(
            DescribeTopicPartitionsResponseData response,
            String topic,
            int partitions,
            Integer nextPartition) {
        if (response.topics().size() != 1
                || !topic.equals(response.topics().iterator().next().name())
                || response.topics().iterator().next().errorCode()
                        != Errors.NONE.code()
                || response.topics().iterator().next().partitions().size()
                        != partitions) {
            throw new AssertionError("unexpected partition page " + response);
        }
        if (nextPartition == null) {
            if (response.nextCursor() != null) {
                throw new AssertionError("unexpected continuation " + response);
            }
        } else if (response.nextCursor() == null
                || !topic.equals(response.nextCursor().topicName())
                || response.nextCursor().partitionIndex() != nextPartition) {
            throw new AssertionError("unexpected continuation " + response);
        }
    }

    private static void assertInvalid(DescribeTopicPartitionsResponseData response) {
        if (response.topics().size() != 1
                || response.topics().iterator().next().errorCode()
                        != Errors.INVALID_REQUEST.code()
                || response.nextCursor() != null) {
            throw new AssertionError("invalid cursor was accepted " + response);
        }
    }
}
