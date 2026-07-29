package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

public final class DeleteTopicsAuthorizationPrivacySmoke {
    private static final short VERSION = 6;
    private static final String PRINCIPAL = "User:ANONYMOUS";

    private DeleteTopicsAuthorizationPrivacySmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "DeleteTopicsAuthorizationPrivacySmoke <bootstrap> <prefix>");
        }
        String bootstrap = args[0];
        String existing = args[1] + "-existing";
        String missing = args[1] + "-missing";

        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(bootstrap))) {
            admin.createTopics(List.of(new NewTopic(existing, 1, (short) 1)))
                    .all()
                    .get(15, TimeUnit.SECONDS);
            Uuid topicId = admin.describeTopics(Set.of(existing))
                    .allTopicNames()
                    .get(15, TimeUnit.SECONDS)
                    .get(existing)
                    .topicId();

            AclBinding existingDeny = binding(
                    existing, AclOperation.ALL, AclPermissionType.DENY);
            AclBinding missingDeny = binding(
                    missing, AclOperation.ALL, AclPermissionType.DENY);
            AclBinding clusterDeleteDeny = binding(
                    ResourceType.CLUSTER,
                    "kafka-cluster",
                    AclOperation.DELETE,
                    AclPermissionType.DENY);
            AclBinding clusterAlterAllow = binding(
                    ResourceType.CLUSTER,
                    "kafka-cluster",
                    AclOperation.ALTER,
                    AclPermissionType.ALLOW);
            admin.createAcls(List.of(
                            existingDeny,
                            missingDeny,
                            clusterDeleteDeny,
                            clusterAlterAllow))
                    .all()
                    .get(15, TimeUnit.SECONDS);

            assertAdminAuthorizationFailure(admin, existing);
            assertNamesAreIndistinguishable(bootstrap, existing, missing);
            assertAliasIsPrivate(bootstrap, existing, topicId);

            admin.deleteAcls(List.of(
                            existingDeny.toFilter(), missingDeny.toFilter()))
                    .all()
                    .get(15, TimeUnit.SECONDS);
            AclBinding deleteAllow = binding(
                    existing, AclOperation.DELETE, AclPermissionType.ALLOW);
            admin.createAcls(List.of(deleteAllow))
                    .all()
                    .get(15, TimeUnit.SECONDS);
            assertAuthorizedAliasConflict(bootstrap, existing, topicId);

            admin.deleteTopics(Set.of(existing)).all().get(15, TimeUnit.SECONDS);
            admin.deleteAcls(List.of(deleteAllow.toFilter()))
                    .all()
                    .get(15, TimeUnit.SECONDS);
            admin.deleteAcls(List.of(
                            clusterDeleteDeny.toFilter(),
                            clusterAlterAllow.toFilter()))
                    .all()
                    .get(15, TimeUnit.SECONDS);
        }

        System.out.printf(
                "Kafka DeleteTopics authorization privacy passed prefix=%s%n",
                args[1]);
    }

    private static void assertAdminAuthorizationFailure(Admin admin, String topic)
            throws Exception {
        try {
            admin.deleteTopics(Set.of(topic)).all().get(15, TimeUnit.SECONDS);
            throw new AssertionError("unauthorized Admin deletion succeeded");
        } catch (ExecutionException expected) {
            if (!(expected.getCause() instanceof TopicAuthorizationException)) {
                throw expected;
            }
        }
    }

    private static void assertNamesAreIndistinguishable(
            String bootstrap, String existing, String missing) throws Exception {
        DeleteTopicsResponseData response = exchange(
                bootstrap,
                List.of(byName(existing), byName(missing)),
                95901);
        if (response.responses().size() != 2
                || response.responses().stream().anyMatch(result ->
                        result.errorCode() != Errors.TOPIC_AUTHORIZATION_FAILED.code()
                                || !Uuid.ZERO_UUID.equals(result.topicId()))) {
            throw new AssertionError(
                    "existing and missing names were distinguishable: " + response);
        }
    }

    private static void assertAliasIsPrivate(
            String bootstrap, String topic, Uuid topicId) throws Exception {
        DeleteTopicsResponseData response = exchange(
                bootstrap, List.of(byName(topic), byId(topicId)), 95902);
        if (response.responses().size() != 2
                || response.responses().stream().anyMatch(result ->
                        result.errorCode()
                                != Errors.TOPIC_AUTHORIZATION_FAILED.code())) {
            throw new AssertionError(
                    "unauthorized alias exposed a mapping error: " + response);
        }
        boolean privateId = response.responses().stream().anyMatch(result ->
                result.name() == null && topicId.equals(result.topicId()));
        boolean privateName = response.responses().stream().anyMatch(result ->
                topic.equals(result.name())
                        && Uuid.ZERO_UUID.equals(result.topicId()));
        if (!privateId || !privateName) {
            throw new AssertionError(
                    "unauthorized alias exposed name-to-ID mapping: " + response);
        }
    }

    private static void assertAuthorizedAliasConflict(
            String bootstrap, String topic, Uuid topicId) throws Exception {
        DeleteTopicsResponseData response = exchange(
                bootstrap, List.of(byName(topic), byId(topicId)), 95903);
        if (response.responses().size() != 1) {
            throw new AssertionError("authorized alias did not collapse: " + response);
        }
        DeletableTopicResult result = response.responses().iterator().next();
        if (result.errorCode() != Errors.INVALID_REQUEST.code()
                || !topic.equals(result.name())
                || !topicId.equals(result.topicId())) {
            throw new AssertionError(
                    "authorized alias returned the wrong identity: " + result);
        }
    }

    private static DeleteTopicsResponseData exchange(
            String bootstrap, List<DeleteTopicState> topics, int correlationId)
            throws Exception {
        DeleteTopicsRequestData data =
                new DeleteTopicsRequestData()
                        .setTopics(topics)
                        .setTimeoutMs(10_000);
        DeleteTopicsRequest request =
                new DeleteTopicsRequest.Builder(data).build(VERSION);
        ByteBuffer body = KafkaWire.exchange(bootstrap, request, correlationId);
        return new DeleteTopicsResponseData(
                new ByteBufferAccessor(body), VERSION);
    }

    private static DeleteTopicState byName(String topic) {
        return new DeleteTopicState()
                .setName(topic)
                .setTopicId(Uuid.ZERO_UUID);
    }

    private static DeleteTopicState byId(Uuid topicId) {
        return new DeleteTopicState().setName(null).setTopicId(topicId);
    }

    private static AclBinding binding(
            String topic, AclOperation operation, AclPermissionType permission) {
        return binding(ResourceType.TOPIC, topic, operation, permission);
    }

    private static AclBinding binding(
            ResourceType resourceType,
            String resourceName,
            AclOperation operation,
            AclPermissionType permission) {
        return new AclBinding(
                new ResourcePattern(
                        resourceType, resourceName, PatternType.LITERAL),
                new AccessControlEntry(
                        PRINCIPAL, "*", operation, permission));
    }
}
