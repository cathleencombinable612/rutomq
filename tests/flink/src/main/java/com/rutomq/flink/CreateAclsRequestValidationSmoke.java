package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateAclsRequestData.AclCreation;
import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

public final class CreateAclsRequestValidationSmoke {
    private static final short VERSION = 3;
    private static final String PRINCIPAL = "User:ANONYMOUS";

    private CreateAclsRequestValidationSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "CreateAclsRequestValidationSmoke <bootstrap> <prefix>");
        }
        String requestWideName = args[1] + "-request-wide";
        String semanticName = args[1] + "-semantic";

        try (Admin admin =
                Admin.create(ConsumerProtocolSmoke.adminProperties(args[0]))) {
            AclCreation malformed =
                    creation(args[1] + "-unknown")
                            .setOperation(AclOperation.UNKNOWN.code());
            CreateAclsResponseData requestWide =
                    exchange(
                            args[0],
                            List.of(creation(requestWideName), malformed),
                            96101);
            if (requestWide.results().size() != 2
                    || requestWide.results().stream()
                            .anyMatch(result ->
                                    result.errorCode()
                                            != Errors.INVALID_REQUEST.code())) {
                throw new AssertionError(
                        "malformed request did not fail every creation: "
                                + requestWide);
            }
            assertAclCount(admin, requestWideName, 0);

            CreateAclsResponseData semantic =
                    exchange(
                            args[0],
                            List.of(creation(semanticName), creation("")),
                            96102);
            if (semantic.results().size() != 2
                    || semantic.results().get(0).errorCode()
                            != Errors.NONE.code()
                    || semantic.results().get(1).errorCode()
                            != Errors.INVALID_REQUEST.code()) {
                throw new AssertionError(
                        "semantic ACL validation was not per entry: " + semantic);
            }
            assertAclCount(admin, semanticName, 1);
            admin.deleteAcls(List.of(filter(semanticName)))
                    .all()
                    .get(15, TimeUnit.SECONDS);
        }

        System.out.printf(
                "Kafka CreateAcls request validation passed prefix=%s%n",
                args[1]);
    }

    private static CreateAclsResponseData exchange(
            String bootstrap, List<AclCreation> creations, int correlationId)
            throws Exception {
        CreateAclsRequestData data =
                new CreateAclsRequestData().setCreations(creations);
        ByteBuffer body =
                KafkaWire.exchangeRaw(
                        bootstrap,
                        ApiKeys.CREATE_ACLS,
                        VERSION,
                        data,
                        correlationId);
        return new CreateAclsResponseData(
                new ByteBufferAccessor(body), VERSION);
    }

    private static AclCreation creation(String resourceName) {
        return new AclCreation()
                .setResourceType(ResourceType.TOPIC.code())
                .setResourceName(resourceName)
                .setResourcePatternType(PatternType.LITERAL.code())
                .setPrincipal(PRINCIPAL)
                .setHost("*")
                .setOperation(AclOperation.READ.code())
                .setPermissionType(AclPermissionType.ALLOW.code());
    }

    private static AclBindingFilter filter(String resourceName) {
        return new AclBindingFilter(
                new ResourcePatternFilter(
                        ResourceType.TOPIC,
                        resourceName,
                        PatternType.LITERAL),
                new AccessControlEntryFilter(
                        PRINCIPAL,
                        "*",
                        AclOperation.READ,
                        AclPermissionType.ALLOW));
    }

    private static void assertAclCount(
            Admin admin, String resourceName, int expected) throws Exception {
        Collection<AclBinding> bindings =
                admin.describeAcls(filter(resourceName))
                        .values()
                        .get(15, TimeUnit.SECONDS);
        if (bindings.size() != expected) {
            throw new AssertionError(
                    "expected "
                            + expected
                            + " ACLs for "
                            + resourceName
                            + " but found "
                            + bindings);
        }
    }
}
