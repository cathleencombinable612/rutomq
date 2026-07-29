package com.rutomq.flink;

import java.nio.ByteBuffer;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.InitProducerIdRequest;

public final class TransactionLegacyFencingSmoke {
    private TransactionLegacyFencingSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "TransactionLegacyFencingSmoke <bootstrap> <prefix>");
        }
        String bootstrap = args[0];
        String transactionalId = args[1] + "-transaction";
        InitProducerIdResponseData first =
                initProducer(bootstrap, transactionalId, (short) 4, -1, (short) -1, 11301);
        assertError("first InitProducerId", first.errorCode(), Errors.NONE);
        InitProducerIdResponseData current =
                initProducer(bootstrap, transactionalId, (short) 4, -1, (short) -1, 11302);
        assertError("second InitProducerId", current.errorCode(), Errors.NONE);
        if (current.producerId() != first.producerId()
                || current.producerEpoch() <= first.producerEpoch()) {
            throw new AssertionError(
                    "InitProducerId did not fence the prior producer: "
                            + first
                            + " -> "
                            + current);
        }

        InitProducerIdResponseData malformed = initProducer(
                bootstrap,
                transactionalId,
                (short) 3,
                first.producerId(),
                (short) -1,
                11303);
        assertError(
                "InitProducerId partial identity",
                malformed.errorCode(),
                Errors.INVALID_REQUEST);
        assertError(
                "InitProducerId v3 fencing",
                initProducer(
                                bootstrap,
                                transactionalId,
                                (short) 3,
                                first.producerId(),
                                first.producerEpoch(),
                                11304)
                        .errorCode(),
                Errors.INVALID_PRODUCER_EPOCH);
        assertError(
                "InitProducerId v4 fencing",
                initProducer(
                                bootstrap,
                                transactionalId,
                                (short) 4,
                                first.producerId(),
                                first.producerEpoch(),
                                11305)
                        .errorCode(),
                Errors.PRODUCER_FENCED);

        for (short version : new short[] {1, 2}) {
            Errors expected =
                    version < 2 ? Errors.INVALID_PRODUCER_EPOCH : Errors.PRODUCER_FENCED;
            assertError(
                    "AddOffsetsToTxn v" + version,
                    addOffsets(
                            bootstrap,
                            transactionalId,
                            first.producerId(),
                            first.producerEpoch(),
                            version,
                            11310 + version),
                    expected);
            assertError(
                    "EndTxn v" + version,
                    endTransaction(
                            bootstrap,
                            transactionalId,
                            first.producerId(),
                            first.producerEpoch(),
                            version,
                            11320 + version),
                    expected);
        }

        System.out.printf(
                "Kafka 4.2 transaction identity and legacy fencing passed prefix=%s%n",
                args[1]);
    }

    private static InitProducerIdResponseData initProducer(
            String bootstrap,
            String transactionalId,
            short version,
            long producerId,
            short producerEpoch,
            int correlationId)
            throws Exception {
        InitProducerIdRequestData data = new InitProducerIdRequestData()
                .setTransactionalId(transactionalId)
                .setTransactionTimeoutMs(60_000)
                .setProducerId(producerId)
                .setProducerEpoch(producerEpoch);
        InitProducerIdRequest request =
                new InitProducerIdRequest.Builder(data).build(version);
        ByteBuffer body = KafkaWire.exchange(bootstrap, request, correlationId);
        return new InitProducerIdResponseData(
                new ByteBufferAccessor(body), version);
    }

    private static short addOffsets(
            String bootstrap,
            String transactionalId,
            long producerId,
            short producerEpoch,
            short version,
            int correlationId)
            throws Exception {
        AddOffsetsToTxnRequestData data = new AddOffsetsToTxnRequestData()
                .setTransactionalId(transactionalId)
                .setProducerId(producerId)
                .setProducerEpoch(producerEpoch)
                .setGroupId(transactionalId + "-group");
        AddOffsetsToTxnRequest request =
                new AddOffsetsToTxnRequest.Builder(data).build(version);
        ByteBuffer body = KafkaWire.exchange(bootstrap, request, correlationId);
        return new AddOffsetsToTxnResponseData(
                        new ByteBufferAccessor(body), version)
                .errorCode();
    }

    private static short endTransaction(
            String bootstrap,
            String transactionalId,
            long producerId,
            short producerEpoch,
            short version,
            int correlationId)
            throws Exception {
        EndTxnRequestData data = new EndTxnRequestData()
                .setTransactionalId(transactionalId)
                .setProducerId(producerId)
                .setProducerEpoch(producerEpoch)
                .setCommitted(false);
        EndTxnRequest request =
                new EndTxnRequest.Builder(data, false).build(version);
        ByteBuffer body = KafkaWire.exchange(bootstrap, request, correlationId);
        return new EndTxnResponseData(new ByteBufferAccessor(body), version)
                .errorCode();
    }

    private static void assertError(String operation, short actual, Errors expected) {
        if (actual != expected.code()) {
            throw new AssertionError(
                    operation
                            + " returned "
                            + Errors.forCode(actual)
                            + ", expected "
                            + expected);
        }
    }
}
