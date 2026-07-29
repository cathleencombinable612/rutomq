package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

public final class MetadataSemanticsSmoke {
    private static final String PRINCIPAL = "User:ANONYMOUS";

    private MetadataSemanticsSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "MetadataSemanticsSmoke <bootstrap> <prefix>");
        }
        String bootstrap = args[0];
        String topic = args[1] + "-known";
        String invalidRequestTopic = args[1] + "-must-not-create";
        String ignoredName = args[1] + "-ignored";
        String deniedName = args[1] + "-denied";
        String defaultName = args[1] + "-controller-default";

        try (Admin admin =
                Admin.create(ConsumerProtocolSmoke.adminProperties(bootstrap))) {
            admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1)))
                    .all()
                    .get(15, TimeUnit.SECONDS);
            Uuid topicId =
                    admin.describeTopics(Set.of(topic))
                            .allTopicNames()
                            .get(15, TimeUnit.SECONDS)
                            .get(topic)
                            .topicId();

            assertVersionZeroEmptyMeansAll(bootstrap, topic);
            assertVersionTenRejectsIds(
                    bootstrap, invalidRequestTopic, Uuid.randomUuid());
            assertVersionTwelveUsesIds(
                    admin, bootstrap, topic, topicId, ignoredName);
            assertInvalidName(bootstrap);
            assertAutoCreationDefaults(admin, bootstrap, defaultName, 3);

            List<AclBinding> fixture =
                    List.of(
                            binding(
                                    ResourceType.CLUSTER,
                                    "kafka-cluster",
                                    AclOperation.ALTER,
                                    AclPermissionType.ALLOW),
                            binding(
                                    ResourceType.CLUSTER,
                                    "kafka-cluster",
                                    AclOperation.DESCRIBE,
                                    AclPermissionType.ALLOW),
                            binding(
                                    ResourceType.CLUSTER,
                                    "kafka-cluster",
                                    AclOperation.CREATE,
                                    AclPermissionType.DENY),
                            binding(
                                    ResourceType.TOPIC,
                                    deniedName,
                                    AclOperation.DESCRIBE,
                                    AclPermissionType.ALLOW),
                            binding(
                                    ResourceType.TOPIC,
                                    deniedName,
                                    AclOperation.CREATE,
                                    AclPermissionType.DENY),
                            binding(
                                    ResourceType.TOPIC,
                                    topic,
                                    AclOperation.READ,
                                    AclPermissionType.ALLOW));
            admin.createAcls(fixture).all().get(15, TimeUnit.SECONDS);
            assertAutoCreationAuthorization(admin, bootstrap, deniedName);
            assertAuthorizedOperations(admin, bootstrap, topic);
            admin.deleteAcls(
                            fixture.stream().map(AclBinding::toFilter).toList())
                    .all()
                    .get(15, TimeUnit.SECONDS);
            admin.deleteTopics(Set.of(topic, defaultName))
                    .all()
                    .get(15, TimeUnit.SECONDS);
        }

        System.out.printf(
                "Kafka Metadata v0/v10-v13 semantics passed prefix=%s%n",
                args[1]);
    }

    private static void assertVersionZeroEmptyMeansAll(
            String bootstrap, String expectedTopic) throws Exception {
        MetadataResponseData v0 =
                exchange(
                        bootstrap,
                        (short) 0,
                        new MetadataRequestData().setTopics(List.of()),
                        96201);
        if (findByName(v0, expectedTopic) == null) {
            throw new AssertionError(
                    "Metadata v0 empty list did not return all topics: " + v0);
        }
        MetadataResponseData v1 =
                exchange(
                        bootstrap,
                        (short) 1,
                        new MetadataRequestData().setTopics(List.of()),
                        96202);
        if (!v1.topics().isEmpty()) {
            throw new AssertionError(
                    "Metadata v1 empty list returned topics: " + v1);
        }
    }

    private static void assertVersionTenRejectsIds(
            String bootstrap, String name, Uuid topicId) throws Exception {
        MetadataRequestData request =
                new MetadataRequestData()
                        .setTopics(
                                List.of(
                                        named(name),
                                        named("invalid-id")
                                                .setTopicId(topicId)))
                        .setAllowAutoTopicCreation(true);
        MetadataResponseData response =
                exchange(bootstrap, (short) 10, request, 96203);
        if (response.topics().size() != 2
                || response.topics().stream()
                        .anyMatch(topic ->
                                topic.errorCode()
                                        != Errors.INVALID_REQUEST.code())) {
            throw new AssertionError(
                    "Metadata v10 accepted a topic ID: " + response);
        }
    }

    private static void assertVersionTwelveUsesIds(
            Admin admin,
            String bootstrap,
            String topic,
            Uuid topicId,
            String ignoredName)
            throws Exception {
        Uuid unknownId = Uuid.randomUuid();
        MetadataRequestData request =
                new MetadataRequestData()
                        .setTopics(
                                List.of(
                                        named("misleading")
                                                .setTopicId(topicId),
                                        named("also-misleading")
                                                .setTopicId(unknownId),
                                        named(ignoredName)))
                        .setAllowAutoTopicCreation(true);
        MetadataResponseData response =
                exchange(bootstrap, (short) 12, request, 96204);
        MetadataResponseTopic known = findById(response, topicId);
        MetadataResponseTopic unknown = findById(response, unknownId);
        if (response.topics().size() != 2
                || known == null
                || known.errorCode() != Errors.NONE.code()
                || !topic.equals(known.name())
                || unknown == null
                || unknown.errorCode() != Errors.UNKNOWN_TOPIC_ID.code()
                || unknown.name() != null) {
            throw new AssertionError(
                    "Metadata v12 topic-ID selection was wrong: " + response);
        }
        if (admin.listTopics()
                .names()
                .get(15, TimeUnit.SECONDS)
                .contains(ignoredName)) {
            throw new AssertionError(
                    "Metadata ID mode auto-created ignored name "
                            + ignoredName);
        }
    }

    private static void assertInvalidName(String bootstrap) throws Exception {
        MetadataResponseData response =
                exchange(
                        bootstrap,
                        (short) 13,
                        new MetadataRequestData()
                                .setTopics(List.of(named("bad/name")))
                                .setAllowAutoTopicCreation(true),
                        96205);
        if (response.topics().size() != 1
                || response.topics().iterator().next().errorCode()
                        != Errors.INVALID_TOPIC_EXCEPTION.code()) {
            throw new AssertionError(
                    "Metadata accepted an invalid topic name: " + response);
        }
    }

    private static void assertAutoCreationAuthorization(
            Admin admin, String bootstrap, String name) throws Exception {
        MetadataResponseData response =
                exchange(
                        bootstrap,
                        (short) 13,
                        new MetadataRequestData()
                                .setTopics(List.of(named(name)))
                                .setAllowAutoTopicCreation(true),
                        96206);
        if (response.topics().size() != 1
                || response.topics().iterator().next().errorCode()
                        != Errors.TOPIC_AUTHORIZATION_FAILED.code()) {
            throw new AssertionError(
                    "Metadata auto-create ignored Create ACLs: " + response);
        }
        if (admin.listTopics()
                .names()
                .get(15, TimeUnit.SECONDS)
                .contains(name)) {
            throw new AssertionError(
                    "unauthorized Metadata request created " + name);
        }
    }

    private static void assertAutoCreationDefaults(
            Admin admin, String bootstrap, String name, int expectedPartitions)
            throws Exception {
        MetadataResponseData response =
                exchange(
                        bootstrap,
                        (short) 13,
                        new MetadataRequestData()
                                .setTopics(List.of(named(name)))
                                .setAllowAutoTopicCreation(true),
                        96208);
        MetadataResponseTopic topic = findByName(response, name);
        if (topic == null
                || topic.errorCode() != Errors.NONE.code()
                || topic.partitions().size() != expectedPartitions) {
            throw new AssertionError(
                    "Metadata did not use the controller partition default: "
                            + response);
        }
        TopicDescription description =
                admin.describeTopics(List.of(name))
                        .allTopicNames()
                        .get(15, TimeUnit.SECONDS)
                        .get(name);
        if (description.partitions().size() != expectedPartitions) {
            throw new AssertionError(
                    "Metadata auto-created "
                            + description.partitions().size()
                            + " partitions instead of "
                            + expectedPartitions);
        }
    }

    private static void assertAuthorizedOperations(
            Admin admin, String bootstrap, String topic) throws Exception {
        MetadataRequestData request =
                new MetadataRequestData()
                        .setTopics(List.of(named(topic)))
                        .setIncludeClusterAuthorizedOperations(true)
                        .setIncludeTopicAuthorizedOperations(true);
        MetadataResponseData response =
                exchange(bootstrap, (short) 10, request, 96207);
        int topicOperations =
                bit(AclOperation.READ) | bit(AclOperation.DESCRIBE);
        int clusterOperations =
                bit(AclOperation.ALTER) | bit(AclOperation.DESCRIBE);
        if (response.topics().size() != 1
                || response.topics()
                                .iterator()
                                .next()
                                .topicAuthorizedOperations()
                        != topicOperations
                || response.clusterAuthorizedOperations()
                        != clusterOperations) {
            throw new AssertionError(
                    "Metadata authorized-operation bits were wrong: "
                            + response);
        }

        TopicDescription description =
                admin.describeTopics(
                                List.of(topic),
                                new DescribeTopicsOptions()
                                        .includeAuthorizedOperations(true))
                        .allTopicNames()
                        .get(15, TimeUnit.SECONDS)
                        .get(topic);
        if (!description.authorizedOperations()
                        .equals(Set.of(
                                AclOperation.READ,
                                AclOperation.DESCRIBE))) {
            throw new AssertionError(
                    "Admin topic authorized operations were wrong: "
                            + description.authorizedOperations());
        }
    }

    private static MetadataResponseData exchange(
            String bootstrap,
            short version,
            MetadataRequestData request,
            int correlationId)
            throws Exception {
        ByteBuffer body =
                KafkaWire.exchangeRaw(
                        bootstrap,
                        ApiKeys.METADATA,
                        version,
                        request,
                        correlationId);
        return new MetadataResponseData(
                new ByteBufferAccessor(body), version);
    }

    private static MetadataRequestTopic named(String name) {
        return new MetadataRequestTopic().setName(name);
    }

    private static MetadataResponseTopic findByName(
            MetadataResponseData response, String name) {
        return response.topics().stream()
                .filter(topic -> name.equals(topic.name()))
                .findFirst()
                .orElse(null);
    }

    private static MetadataResponseTopic findById(
            MetadataResponseData response, Uuid topicId) {
        return response.topics().stream()
                .filter(topic -> topicId.equals(topic.topicId()))
                .findFirst()
                .orElse(null);
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

    private static int bit(AclOperation operation) {
        return 1 << operation.code();
    }
}
