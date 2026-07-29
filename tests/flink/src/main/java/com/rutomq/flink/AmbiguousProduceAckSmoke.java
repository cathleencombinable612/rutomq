package com.rutomq.flink;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.ProduceRequest;

public final class AmbiguousProduceAckSmoke {
    private static final short INIT_PRODUCER_ID_VERSION = 4;
    private static final short PRODUCE_VERSION = 12;
    private static final String CLIENT_ID = "rutomq-ambiguous-ack";
    private static final long RECORD_TIMESTAMP = 1_750_000_000_000L;

    private AmbiguousProduceAckSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            throw new IllegalArgumentException(
                    "AmbiguousProduceAckSmoke <first|retry> <agent> <topic> <value> "
                            + "[producer-id producer-epoch expected-offset]");
        }
        switch (args[0]) {
            case "first" -> first(args[1], args[2], args[3]);
            case "retry" -> {
                if (args.length != 7) {
                    throw new IllegalArgumentException(
                            "retry requires producer-id producer-epoch expected-offset");
                }
                retry(
                        args[1],
                        args[2],
                        args[3],
                        Long.parseLong(args[4]),
                        Short.parseShort(args[5]),
                        Long.parseLong(args[6]));
            }
            default -> throw new IllegalArgumentException("unknown mode " + args[0]);
        }
    }

    private static void first(String agent, String topic, String value)
            throws Exception {
        InitProducerIdRequestData initData =
                new InitProducerIdRequestData()
                        .setTransactionalId(null)
                        .setTransactionTimeoutMs(60_000)
                        .setProducerId(-1)
                        .setProducerEpoch((short) -1);
        InitProducerIdRequest initRequest =
                new InitProducerIdRequest.Builder(initData)
                        .build(INIT_PRODUCER_ID_VERSION);
        ByteBuffer initBody = KafkaWire.exchange(agent, initRequest, 200001);
        InitProducerIdResponseData initialized =
                new InitProducerIdResponseData(
                        new ByteBufferAccessor(initBody),
                        INIT_PRODUCER_ID_VERSION);
        assertError("InitProducerId", initialized.errorCode());

        try {
            KafkaWire.exchange(
                    agent,
                    produceRequest(
                            topic,
                            value,
                            initialized.producerId(),
                            initialized.producerEpoch()),
                    200002,
                    CLIENT_ID);
        } catch (EOFException expected) {
            System.out.printf(
                    "AMBIGUOUS_FIRST producer_id=%d producer_epoch=%d sequence=0 eof=true%n",
                    initialized.producerId(),
                    initialized.producerEpoch());
            return;
        }
        throw new AssertionError(
                "first Produce unexpectedly received a response");
    }

    private static void retry(
            String agent,
            String topic,
            String value,
            long producerId,
            short producerEpoch,
            long expectedOffset)
            throws Exception {
        ByteBuffer body =
                KafkaWire.exchange(
                        agent,
                        produceRequest(topic, value, producerId, producerEpoch),
                        200003,
                        CLIENT_ID);
        ProduceResponseData response =
                new ProduceResponseData(
                        new ByteBufferAccessor(body), PRODUCE_VERSION);
        var topics = response.responses().iterator();
        if (!topics.hasNext()) {
            throw new AssertionError(
                    "unexpected Produce retry response " + response);
        }
        ProduceResponseData.TopicProduceResponse topicResponse = topics.next();
        if (topics.hasNext()
                || topicResponse.partitionResponses().size() != 1) {
            throw new AssertionError(
                    "unexpected Produce retry response " + response);
        }
        ProduceResponseData.PartitionProduceResponse partition =
                topicResponse.partitionResponses().get(0);
        assertError("Produce retry", partition.errorCode());
        if (partition.baseOffset() != expectedOffset) {
            throw new AssertionError(
                    "retry expected base offset "
                            + expectedOffset
                            + ", got "
                            + partition.baseOffset());
        }
        System.out.printf(
                "AMBIGUOUS_RETRY producer_id=%d producer_epoch=%d sequence=0 base_offset=%d%n",
                producerId, producerEpoch, partition.baseOffset());
    }

    private static ProduceRequest produceRequest(
            String topic, String value, long producerId, short producerEpoch) {
        MemoryRecords records =
                MemoryRecords.withIdempotentRecords(
                        Compression.NONE,
                        producerId,
                        producerEpoch,
                        0,
                        new SimpleRecord(
                                RECORD_TIMESTAMP,
                                value.getBytes(StandardCharsets.UTF_8)));
        ProduceRequestData.TopicProduceData topicData =
                new ProduceRequestData.TopicProduceData()
                        .setName(topic)
                        .setPartitionData(
                                List.of(
                                        new ProduceRequestData.PartitionProduceData()
                                                .setIndex(0)
                                                .setRecords(records)));
        ProduceRequestData request =
                new ProduceRequestData()
                        .setAcks((short) -1)
                        .setTimeoutMs(30_000)
                        .setTopicData(
                                new ProduceRequestData.TopicProduceDataCollection(
                                        List.of(topicData).iterator()));
        return new ProduceRequest.Builder(
                        PRODUCE_VERSION, PRODUCE_VERSION, request)
                .build(PRODUCE_VERSION);
    }

    private static void assertError(String operation, short errorCode) {
        if (errorCode != Errors.NONE.code()) {
            throw new AssertionError(
                    operation
                            + " failed with "
                            + Errors.forCode(errorCode));
        }
    }
}
