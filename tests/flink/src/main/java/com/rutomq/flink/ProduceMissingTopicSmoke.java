package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;

public final class ProduceMissingTopicSmoke {
    private static final short VERSION = 12;

    private ProduceMissingTopicSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "ProduceMissingTopicSmoke <bootstrap> <prefix>");
        }
        String existing = args[1] + "-existing";
        String missing = args[1] + "-missing";
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(args[0]))) {
            admin.createTopics(List.of(new NewTopic(existing, 1, (short) 1)))
                    .all()
                    .get(15, TimeUnit.SECONDS);

            ProduceResponseData response = exchange(
                    args[0],
                    List.of(
                            topic(existing, partition(0, "accepted"), partition(3, "invalid")),
                            topic(missing, partition(0, "must-not-create"))));
            ProduceResponseData.TopicProduceResponse existingResult =
                    topicResponse(response, existing);
            assertPartition(existingResult, 0, Errors.NONE, 0);
            assertPartition(
                    existingResult,
                    3,
                    Errors.UNKNOWN_TOPIC_OR_PARTITION,
                    -1);
            assertPartition(
                    topicResponse(response, missing),
                    0,
                    Errors.UNKNOWN_TOPIC_OR_PARTITION,
                    -1);

            Set<String> names =
                    admin.listTopics().names().get(15, TimeUnit.SECONDS);
            if (names.contains(missing)) {
                throw new AssertionError("Produce implicitly created " + missing);
            }
        }
        System.out.printf(
                "Kafka Produce missing topic/partition boundary passed prefix=%s%n",
                args[1]);
    }

    private static ProduceResponseData exchange(
            String bootstrap, List<ProduceRequestData.TopicProduceData> topics)
            throws Exception {
        ProduceRequestData data =
                new ProduceRequestData()
                        .setAcks((short) 1)
                        .setTimeoutMs(10_000)
                        .setTopicData(
                                new ProduceRequestData.TopicProduceDataCollection(
                                        topics.iterator()));
        ProduceRequest request =
                new ProduceRequest.Builder(VERSION, VERSION, data).build(VERSION);
        ByteBuffer body = KafkaWire.exchange(bootstrap, request, 96001);
        return new ProduceResponseData(new ByteBufferAccessor(body), VERSION);
    }

    private static ProduceRequestData.TopicProduceData topic(
            String name, ProduceRequestData.PartitionProduceData... partitions) {
        return new ProduceRequestData.TopicProduceData()
                .setName(name)
                .setPartitionData(List.of(partitions));
    }

    private static ProduceRequestData.PartitionProduceData partition(
            int index, String value) {
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(value.getBytes(StandardCharsets.UTF_8)));
        return new ProduceRequestData.PartitionProduceData()
                .setIndex(index)
                .setRecords(records);
    }

    private static ProduceResponseData.TopicProduceResponse topicResponse(
            ProduceResponseData response, String topic) {
        for (ProduceResponseData.TopicProduceResponse result :
                response.responses()) {
            if (topic.equals(result.name())) {
                return result;
            }
        }
        throw new AssertionError("missing Produce response for " + topic);
    }

    private static void assertPartition(
            ProduceResponseData.TopicProduceResponse topic,
            int index,
            Errors expected,
            long baseOffset) {
        for (ProduceResponseData.PartitionProduceResponse partition :
                topic.partitionResponses()) {
            if (partition.index() == index) {
                if (partition.errorCode() != expected.code()
                        || partition.baseOffset() != baseOffset) {
                    throw new AssertionError(
                            "partition "
                                    + index
                                    + " expected "
                                    + expected
                                    + "/"
                                    + baseOffset
                                    + " but got "
                                    + Errors.forCode(partition.errorCode())
                                    + "/"
                                    + partition.baseOffset());
                }
                return;
            }
        }
        throw new AssertionError("missing partition response " + index);
    }
}
