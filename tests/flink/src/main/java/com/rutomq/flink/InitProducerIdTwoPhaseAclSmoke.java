package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

public final class InitProducerIdTwoPhaseAclSmoke {
    private static final short VERSION = 6;
    private static final String PRINCIPAL = "User:ANONYMOUS";

    private InitProducerIdTwoPhaseAclSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "InitProducerIdTwoPhaseAclSmoke <bootstrap> <prefix>");
        }
        String bootstrap = args[0];
        String writeId = args[1] + "-write";
        String twoPhaseId = args[1] + "-two-phase";
        String allId = args[1] + "-all";
        List<AclBinding> bindings = new ArrayList<>(List.of(
                binding(writeId, AclOperation.WRITE),
                binding(twoPhaseId, AclOperation.TWO_PHASE_COMMIT),
                binding(allId, AclOperation.ALL)));

        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            admin.createAcls(bindings).all().get(15, TimeUnit.SECONDS);
            assertDescribed(admin, bindings);

            assertError(
                    "WRITE permits ordinary InitProducerId",
                    initProducerId(bootstrap, writeId, false, 95801),
                    Errors.NONE);
            assertError(
                    "WRITE alone rejects two-phase InitProducerId",
                    initProducerId(bootstrap, writeId, true, 95802),
                    Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
            assertError(
                    "TWO_PHASE_COMMIT does not imply WRITE",
                    initProducerId(bootstrap, twoPhaseId, true, 95803),
                    Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
            assertError(
                    "ALL permits two-phase InitProducerId",
                    initProducerId(bootstrap, allId, true, 95804),
                    Errors.NONE);

            AclBinding twoPhaseWrite =
                    binding(writeId, AclOperation.TWO_PHASE_COMMIT);
            admin.createAcls(List.of(twoPhaseWrite))
                    .all()
                    .get(15, TimeUnit.SECONDS);
            bindings.add(twoPhaseWrite);
            assertError(
                    "WRITE plus TWO_PHASE_COMMIT permits two-phase InitProducerId",
                    initProducerId(bootstrap, writeId, true, 95805),
                    Errors.NONE);
            assertDescribed(admin, bindings);

            admin.deleteAcls(bindings.stream().map(AclBinding::toFilter).toList())
                    .all()
                    .get(15, TimeUnit.SECONDS);
            var remaining = admin.describeAcls(AclBindingFilter.ANY)
                    .values()
                    .get(15, TimeUnit.SECONDS);
            if (remaining.stream().anyMatch(bindings::contains)) {
                throw new AssertionError("DeleteAcls retained a two-phase ACL");
            }
        }

        System.out.printf(
                "Kafka 4.2 InitProducerId v6 TWO_PHASE_COMMIT ACL passed prefix=%s%n",
                args[1]);
    }

    private static InitProducerIdResponseData initProducerId(
            String bootstrap, String transactionalId, boolean enable2Pc, int correlationId)
            throws Exception {
        InitProducerIdRequestData data =
                new InitProducerIdRequestData()
                        .setTransactionalId(transactionalId)
                        .setTransactionTimeoutMs(5_000)
                        .setProducerId(-1)
                        .setProducerEpoch((short) -1)
                        .setEnable2Pc(enable2Pc)
                        .setKeepPreparedTxn(false);
        InitProducerIdRequest request =
                new InitProducerIdRequest.Builder(data).build(VERSION);
        ByteBuffer body = KafkaWire.exchange(bootstrap, request, correlationId);
        return new InitProducerIdResponseData(
                new ByteBufferAccessor(body), VERSION);
    }

    private static void assertError(
            String operation, InitProducerIdResponseData response, Errors expected) {
        if (response.errorCode() != expected.code()) {
            throw new AssertionError(
                    operation
                            + " expected "
                            + expected
                            + " but got "
                            + Errors.forCode(response.errorCode()));
        }
    }

    private static void assertDescribed(Admin admin, List<AclBinding> expected)
            throws Exception {
        var described = admin.describeAcls(AclBindingFilter.ANY)
                .values()
                .get(15, TimeUnit.SECONDS);
        if (!described.containsAll(expected)) {
            throw new AssertionError(
                    "DescribeAcls omitted TWO_PHASE_COMMIT bindings: " + described);
        }
    }

    private static AclBinding binding(
            String transactionalId, AclOperation operation) {
        return new AclBinding(
                new ResourcePattern(
                        ResourceType.TRANSACTIONAL_ID,
                        transactionalId,
                        PatternType.LITERAL),
                new AccessControlEntry(
                        PRINCIPAL,
                        "*",
                        operation,
                        AclPermissionType.ALLOW));
    }

    private static Properties adminProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        return properties;
    }
}
