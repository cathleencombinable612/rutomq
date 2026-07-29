package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData.Coordinator;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

public final class FindCoordinatorSemanticsSmoke {
    private static final String VALID_SHARE_KEY =
            "share-group:AAAAAAAAAAAAAAAAAAAAAA:0";

    private FindCoordinatorSemanticsSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    "FindCoordinatorSemanticsSmoke <bootstrap>");
        }
        String bootstrap = args[0];

        FindCoordinatorResponseData empty = exchange(
                bootstrap,
                new FindCoordinatorRequestData()
                        .setKeyType((byte) 0)
                        .setCoordinatorKeys(List.of()),
                (short) 4,
                11501);
        if (!empty.coordinators().isEmpty()) {
            throw new AssertionError(
                    "FindCoordinator v4 empty batch returned entries: " + empty);
        }

        FindCoordinatorResponseData duplicates = exchange(
                bootstrap,
                new FindCoordinatorRequestData()
                        .setKeyType((byte) 0)
                        .setCoordinatorKeys(List.of("workers", "workers")),
                (short) 4,
                11502);
        if (duplicates.coordinators().size() != 2) {
            throw new AssertionError(
                    "FindCoordinator did not preserve duplicate keys: " + duplicates);
        }
        for (Coordinator coordinator : duplicates.coordinators()) {
            assertError("duplicate group key", coordinator, Errors.NONE);
            if (!"workers".equals(coordinator.key()) || coordinator.nodeId() != 0) {
                throw new AssertionError(
                        "FindCoordinator changed duplicate group result: " + coordinator);
            }
        }

        FindCoordinatorResponseData unknown = exchange(
                bootstrap,
                new FindCoordinatorRequestData()
                        .setKeyType((byte) 9)
                        .setCoordinatorKeys(List.of("unknown-a", "unknown-b")),
                (short) 4,
                11503);
        if (unknown.coordinators().size() != 2) {
            throw new AssertionError(
                    "FindCoordinator changed unknown-type cardinality: " + unknown);
        }
        for (Coordinator coordinator : unknown.coordinators()) {
            assertError("unknown coordinator type", coordinator, Errors.INVALID_REQUEST);
            assertNoNode(coordinator);
        }

        Coordinator oldShare = onlyCoordinator(exchange(
                bootstrap,
                new FindCoordinatorRequestData()
                        .setKeyType((byte) 2)
                        .setCoordinatorKeys(List.of(VALID_SHARE_KEY)),
                (short) 5,
                11504));
        assertError("FindCoordinator share v5", oldShare, Errors.INVALID_REQUEST);
        assertNoNode(oldShare);

        Coordinator validShare = onlyCoordinator(exchange(
                bootstrap,
                new FindCoordinatorRequestData()
                        .setKeyType((byte) 2)
                        .setCoordinatorKeys(List.of(VALID_SHARE_KEY)),
                (short) 6,
                11505));
        assertError("FindCoordinator share v6", validShare, Errors.NONE);
        if (validShare.nodeId() != 0 || validShare.host().isEmpty() || validShare.port() <= 0) {
            throw new AssertionError(
                    "FindCoordinator share v6 returned no routable coordinator: "
                            + validShare);
        }

        Coordinator malformedShare = onlyCoordinator(exchange(
                bootstrap,
                new FindCoordinatorRequestData()
                        .setKeyType((byte) 2)
                        .setCoordinatorKeys(List.of("share-group:not-a-topic-id:0")),
                (short) 6,
                11506));
        assertError(
                "FindCoordinator malformed share key",
                malformedShare,
                Errors.INVALID_REQUEST);
        assertNoNode(malformedShare);

        System.out.println(
                "Kafka 4.2 FindCoordinator batching, type, share-key, and no-node semantics passed");
    }

    private static FindCoordinatorResponseData exchange(
            String bootstrap,
            FindCoordinatorRequestData request,
            short version,
            int correlationId)
            throws Exception {
        ByteBuffer body = KafkaWire.exchangeRaw(
                bootstrap,
                ApiKeys.FIND_COORDINATOR,
                version,
                request,
                correlationId);
        return new FindCoordinatorResponseData(new ByteBufferAccessor(body), version);
    }

    private static Coordinator onlyCoordinator(FindCoordinatorResponseData response) {
        if (response.coordinators().size() != 1) {
            throw new AssertionError(
                    "expected exactly one coordinator result: " + response);
        }
        return response.coordinators().get(0);
    }

    private static void assertError(
            String operation, Coordinator coordinator, Errors expected) {
        if (coordinator.errorCode() != expected.code()) {
            throw new AssertionError(
                    operation
                            + " returned "
                            + Errors.forCode(coordinator.errorCode())
                            + ", expected "
                            + expected
                            + ": "
                            + coordinator);
        }
    }

    private static void assertNoNode(Coordinator coordinator) {
        if (coordinator.nodeId() != -1
                || !"".equals(coordinator.host())
                || coordinator.port() != -1) {
            throw new AssertionError(
                    "coordinator error exposed a routable endpoint: " + coordinator);
        }
    }
}
