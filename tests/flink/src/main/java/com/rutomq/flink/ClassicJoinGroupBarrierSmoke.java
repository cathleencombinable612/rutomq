package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

public final class ClassicJoinGroupBarrierSmoke {
    private static final short VERSION = 9;

    private ClassicJoinGroupBarrierSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "ClassicJoinGroupBarrierSmoke <bootstrap> <group>");
        }
        String bootstrap = args[0];
        String group = args[1];
        String firstId = allocate(bootstrap, group, 96401);
        String secondId = allocate(bootstrap, group, 96402);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            long startedAt = System.nanoTime();
            Future<JoinGroupResponseData> first =
                    executor.submit(
                            () ->
                                    join(
                                            bootstrap,
                                            request(group, firstId),
                                            96403));
            Thread.sleep(200);
            if (first.isDone()) {
                throw new AssertionError(
                        "first JoinGroup completed before the generation barrier");
            }
            Future<JoinGroupResponseData> second =
                    executor.submit(
                            () ->
                                    join(
                                            bootstrap,
                                            request(group, secondId),
                                            96404));
            JoinGroupResponseData firstResponse =
                    first.get(10, TimeUnit.SECONDS);
            JoinGroupResponseData secondResponse =
                    second.get(10, TimeUnit.SECONDS);
            assertOk(firstResponse, "first member");
            assertOk(secondResponse, "second member");
            if (firstResponse.generationId() != 1
                    || secondResponse.generationId() != 1
                    || !firstResponse.leader().equals(secondResponse.leader())) {
                throw new AssertionError(
                        "JoinGroup responses disagree: "
                                + firstResponse
                                + " / "
                                + secondResponse);
            }
            JoinGroupResponseData leader =
                    firstResponse.memberId().equals(firstResponse.leader())
                            ? firstResponse
                            : secondResponse;
            if (leader.members().size() != 2) {
                throw new AssertionError(
                        "leader did not receive the complete generation: " + leader);
            }
            long elapsedMs =
                    TimeUnit.NANOSECONDS.toMillis(
                            System.nanoTime() - startedAt);
            if (elapsedMs < 200) {
                throw new AssertionError(
                        "JoinGroup barrier elapsed too early: " + elapsedMs);
            }
            System.out.printf(
                    "Kafka classic JoinGroup barrier passed group=%s generation=%d elapsedMs=%d%n",
                    group, leader.generationId(), elapsedMs);
        } finally {
            executor.shutdownNow();
        }
    }

    private static String allocate(
            String bootstrap, String group, int correlationId) throws Exception {
        JoinGroupResponseData response =
                join(bootstrap, request(group, ""), correlationId);
        if (response.errorCode() != Errors.MEMBER_ID_REQUIRED.code()
                || response.memberId().isEmpty()) {
            throw new AssertionError(
                    "member-id allocation failed: " + response);
        }
        return response.memberId();
    }

    private static JoinGroupRequestData request(String group, String memberId) {
        JoinGroupRequestData request =
                new JoinGroupRequestData()
                        .setGroupId(group)
                        .setSessionTimeoutMs(6_000)
                        .setRebalanceTimeoutMs(6_000)
                        .setMemberId(memberId)
                        .setProtocolType("consumer");
        request.protocols().add(
                new JoinGroupRequestData.JoinGroupRequestProtocol()
                        .setName("range")
                        .setMetadata(new byte[] {0, 1, 2, 3}));
        return request;
    }

    private static JoinGroupResponseData join(
            String bootstrap,
            JoinGroupRequestData request,
            int correlationId)
            throws Exception {
        ByteBuffer body =
                KafkaWire.exchangeRaw(
                        bootstrap,
                        ApiKeys.JOIN_GROUP,
                        VERSION,
                        request,
                        correlationId);
        return new JoinGroupResponseData(
                new ByteBufferAccessor(body), VERSION);
    }

    private static void assertOk(
            JoinGroupResponseData response, String member) {
        if (response.errorCode() != Errors.NONE.code()) {
            throw new AssertionError(
                    member + " JoinGroup failed: " + response);
        }
    }
}
