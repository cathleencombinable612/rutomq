package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

public final class TransactionQuerySemanticsSmoke {
    private TransactionQuerySemanticsSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "TransactionQuerySemanticsSmoke <bootstrap> <prefix>");
        }
        String bootstrap = args[0];
        String exactId = args[1] + "-orders";
        String substringId = "x" + exactId;
        initialize(bootstrap, exactId, 11701);
        initialize(bootstrap, substringId, 11702);

        ListTransactionsResponseData unknownOnly = list(
                bootstrap,
                new ListTransactionsRequestData()
                        .setStateFilters(List.of("Bogus")),
                11703);
        assertError("unknown-only state filter", unknownOnly.errorCode(), Errors.NONE);
        if (!unknownOnly.unknownStateFilters().equals(List.of("Bogus"))
                || !unknownOnly.transactionStates().isEmpty()) {
            throw new AssertionError(
                    "unknown-only state filter leaked transactions: " + unknownOnly);
        }

        ListTransactionsResponseData exact = list(
                bootstrap,
                new ListTransactionsRequestData()
                        .setTransactionalIdPattern(exactId),
                11704);
        assertError("full transactional ID pattern", exact.errorCode(), Errors.NONE);
        if (exact.transactionStates().size() != 1
                || !exactId.equals(
                        exact.transactionStates().get(0).transactionalId())) {
            throw new AssertionError(
                    "transactional ID pattern used substring matching: " + exact);
        }

        ListTransactionsResponseData malformed = list(
                bootstrap,
                new ListTransactionsRequestData()
                        .setTransactionalIdPattern("["),
                11705);
        assertError(
                "invalid transactional ID pattern",
                malformed.errorCode(),
                Errors.INVALID_REGULAR_EXPRESSION);
        if (!malformed.transactionStates().isEmpty()) {
            throw new AssertionError(
                    "invalid transactional ID pattern returned transactions: "
                            + malformed);
        }

        DescribeTransactionsRequestData emptyRequest =
                new DescribeTransactionsRequestData()
                        .setTransactionalIds(List.of(""));
        ByteBuffer emptyBody = KafkaWire.exchangeRaw(
                bootstrap,
                ApiKeys.DESCRIBE_TRANSACTIONS,
                (short) 0,
                emptyRequest,
                11706);
        DescribeTransactionsResponseData empty = new DescribeTransactionsResponseData(
                new ByteBufferAccessor(emptyBody), (short) 0);
        if (empty.transactionStates().size() != 1) {
            throw new AssertionError(
                    "empty transactional ID changed response cardinality: " + empty);
        }
        assertError(
                "empty transactional ID",
                empty.transactionStates().get(0).errorCode(),
                Errors.INVALID_REQUEST);

        System.out.printf(
                "Kafka 4.2 transaction query filters and validation passed prefix=%s%n",
                args[1]);
    }

    private static void initialize(
            String bootstrap, String transactionalId, int correlationId)
            throws Exception {
        InitProducerIdRequestData request = new InitProducerIdRequestData()
                .setTransactionalId(transactionalId)
                .setTransactionTimeoutMs(60_000)
                .setProducerId(-1)
                .setProducerEpoch((short) -1);
        ByteBuffer body = KafkaWire.exchangeRaw(
                bootstrap,
                ApiKeys.INIT_PRODUCER_ID,
                (short) 4,
                request,
                correlationId);
        InitProducerIdResponseData response = new InitProducerIdResponseData(
                new ByteBufferAccessor(body), (short) 4);
        assertError(
                "InitProducerId " + transactionalId,
                response.errorCode(),
                Errors.NONE);
    }

    private static ListTransactionsResponseData list(
            String bootstrap,
            ListTransactionsRequestData request,
            int correlationId)
            throws Exception {
        ByteBuffer body = KafkaWire.exchangeRaw(
                bootstrap,
                ApiKeys.LIST_TRANSACTIONS,
                (short) 2,
                request,
                correlationId);
        return new ListTransactionsResponseData(
                new ByteBufferAccessor(body), (short) 2);
    }

    private static void assertError(
            String operation, short actual, Errors expected) {
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
