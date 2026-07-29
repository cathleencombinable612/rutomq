package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterConfigsRequestData.AlterConfigsResource;
import org.apache.kafka.common.message.AlterConfigsRequestData.AlterableConfig;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.message.AlterConfigsResponseData.AlterConfigsResourceResponse;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AlterConfigsRequest;

public final class UnsupportedTopicConfigSmoke {
    private static final short ALTER_CONFIGS_VERSION = 2;
    private static final long TIMEOUT_SECONDS = 15;

    private UnsupportedTopicConfigSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "UnsupportedTopicConfigSmoke <bootstrap> <topic-prefix>");
        }
        String createTopic = args[1] + "-create";
        String validateTopic = args[1] + "-validate";
        String atomicTopic = args[1] + "-atomic";
        String independentTopic = args[1] + "-independent";
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(args[0]))) {
            assertCreateRejected(admin, createTopic, "segment.bytes");
            assertValidateOnlyCreateRejected(admin, validateTopic);
            createTopics(admin, atomicTopic, independentTopic);
            assertIncrementalAtomicity(admin, atomicTopic, independentTopic);
            assertValidateOnlyIncrementalRejected(admin, atomicTopic);
            assertLegacyAlterRejected(args[0], admin, atomicTopic, false, 56001);
            assertLegacyAlterRejected(args[0], admin, atomicTopic, true, 56002);
        }
        System.out.printf(
                "Kafka unsupported physical topic configs rejected atomically prefix=%s%n",
                args[1]);
    }

    private static void assertCreateRejected(Admin admin, String topic, String config)
            throws Exception {
        KafkaFuture<Void> future = admin.createTopics(List.of(
                        new NewTopic(topic, 1, (short) 1)
                                .configs(Map.of(config, "1048576"))))
                .values()
                .get(topic);
        expectInvalidConfiguration(future, "CreateTopics " + config);
        assertTopicAbsent(admin, topic);
    }

    private static void assertValidateOnlyCreateRejected(Admin admin, String topic)
            throws Exception {
        KafkaFuture<Void> future = admin.createTopics(
                        List.of(new NewTopic(topic, 1, (short) 1)
                                .configs(Map.of("segment.index.bytes", "1048576"))),
                        new CreateTopicsOptions().validateOnly(true))
                .values()
                .get(topic);
        expectInvalidConfiguration(future, "validate-only CreateTopics");
        assertTopicAbsent(admin, topic);
    }

    private static void createTopics(Admin admin, String atomicTopic, String independentTopic)
            throws Exception {
        admin.createTopics(List.of(
                        configuredTopic(atomicTopic, "600000"),
                        configuredTopic(independentTopic, "600000")))
                .all()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static NewTopic configuredTopic(String name, String retentionMs) {
        return new NewTopic(name, 1, (short) 1)
                .configs(Map.of("retention.ms", retentionMs));
    }

    private static void assertIncrementalAtomicity(
            Admin admin, String atomicTopic, String independentTopic) throws Exception {
        ConfigResource atomic = topicResource(atomicTopic);
        ConfigResource independent = topicResource(independentTopic);
        Map<ConfigResource, Collection<AlterConfigOp>> operations =
                new LinkedHashMap<>();
        operations.put(
                atomic,
                List.of(
                        set("retention.ms", "1"),
                        set("remote.storage.enable", "true")));
        operations.put(independent, List.of(set("retention.ms", "700000")));

        var result = admin.incrementalAlterConfigs(operations);
        expectInvalidConfiguration(result.values().get(atomic), "IncrementalAlterConfigs");
        result.values()
                .get(independent)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertConfig(admin, atomic, "retention.ms", "600000");
        assertConfig(admin, independent, "retention.ms", "700000");
    }

    private static void assertValidateOnlyIncrementalRejected(
            Admin admin, String topic) throws Exception {
        ConfigResource resource = topicResource(topic);
        var result = admin.incrementalAlterConfigs(
                Map.of(
                        resource,
                        List.of(
                                set("retention.ms", "2"),
                                delete("local.retention.ms"))),
                new AlterConfigsOptions().validateOnly(true));
        expectInvalidConfiguration(
                result.values().get(resource),
                "validate-only IncrementalAlterConfigs");
        assertConfig(admin, resource, "retention.ms", "600000");
    }

    private static void assertLegacyAlterRejected(
            String bootstrap,
            Admin admin,
            String topic,
            boolean validateOnly,
            int correlationId)
            throws Exception {
        AlterConfigsResource resource = new AlterConfigsResource()
                .setResourceType(ConfigResource.Type.TOPIC.id())
                .setResourceName(topic);
        resource.configs().add(legacyConfig("retention.ms", "3"));
        resource.configs().add(legacyConfig("index.interval.bytes", "4096"));
        AlterConfigsRequestData data =
                new AlterConfigsRequestData().setValidateOnly(validateOnly);
        data.resources().add(resource);
        AlterConfigsRequest request =
                new AlterConfigsRequest(data, ALTER_CONFIGS_VERSION);
        ByteBuffer responseBody =
                KafkaWire.exchange(bootstrap, request, correlationId);
        AlterConfigsResponseData response = new AlterConfigsResponseData(
                new ByteBufferAccessor(responseBody), ALTER_CONFIGS_VERSION);
        if (response.responses().size() != 1) {
            throw new AssertionError("unexpected AlterConfigs response " + response);
        }
        AlterConfigsResourceResponse result = response.responses().get(0);
        if (result.errorCode() != Errors.INVALID_CONFIG.code()
                || result.errorMessage() == null
                || !result.errorMessage().contains("stateless object-storage")) {
            throw new AssertionError("unsupported legacy config was accepted " + result);
        }
        assertConfig(admin, topicResource(topic), "retention.ms", "600000");
    }

    private static AlterableConfig legacyConfig(String name, String value) {
        return new AlterableConfig().setName(name).setValue(value);
    }

    private static AlterConfigOp set(String name, String value) {
        return new AlterConfigOp(
                new ConfigEntry(name, value), AlterConfigOp.OpType.SET);
    }

    private static AlterConfigOp delete(String name) {
        return new AlterConfigOp(
                new ConfigEntry(name, null), AlterConfigOp.OpType.DELETE);
    }

    private static ConfigResource topicResource(String topic) {
        return new ConfigResource(ConfigResource.Type.TOPIC, topic);
    }

    private static void expectInvalidConfiguration(
            KafkaFuture<?> future, String operation) throws Exception {
        try {
            future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            throw new AssertionError(operation + " accepted an unsupported config");
        } catch (ExecutionException expected) {
            if (!(expected.getCause() instanceof InvalidConfigurationException)) {
                throw new AssertionError(
                        operation + " returned " + expected.getCause(), expected);
            }
        }
    }

    private static void assertTopicAbsent(Admin admin, String topic)
            throws Exception {
        if (admin.listTopics()
                .names()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .contains(topic)) {
            throw new AssertionError("rejected topic was created: " + topic);
        }
    }

    private static void assertConfig(
            Admin admin, ConfigResource resource, String name, String expected)
            throws Exception {
        Config config = admin.describeConfigs(List.of(resource))
                .all()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .get(resource);
        ConfigEntry entry = config.get(name);
        if (entry == null || !expected.equals(entry.value())) {
            throw new AssertionError(
                    "unexpected "
                            + name
                            + " for "
                            + resource
                            + ": "
                            + (entry == null ? null : entry.value()));
        }
    }
}
