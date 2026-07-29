package com.rutomq.flink;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;

public final class TopicFlushPolicySmoke {
    private static final String MESSAGES = "flush.messages";
    private static final String MILLIS = "flush.ms";
    private static final String DEFAULT = Long.toString(Long.MAX_VALUE);

    private TopicFlushPolicySmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("TopicFlushPolicySmoke <bootstrap> <topic>");
        }
        String topic = args[1];
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(args[0]))) {
            admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1)
                            .configs(Map.of(MESSAGES, "7", MILLIS, "11"))))
                    .all()
                    .get(15, TimeUnit.SECONDS);
            assertValues(
                    admin,
                    resource,
                    "7",
                    "11",
                    ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);

            reject(admin, resource, MESSAGES, "0");
            reject(admin, resource, MILLIS, "-1");
            assertValues(
                    admin,
                    resource,
                    "7",
                    "11",
                    ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);

            alter(admin, resource, List.of(
                    set(MESSAGES, "3"),
                    set(MILLIS, "0")));
            assertValues(
                    admin,
                    resource,
                    "3",
                    "0",
                    ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);
            alter(admin, resource, List.of(delete(MESSAGES), delete(MILLIS)));
            assertValues(
                    admin,
                    resource,
                    DEFAULT,
                    DEFAULT,
                    ConfigEntry.ConfigSource.DEFAULT_CONFIG);
        }
        System.out.printf(
                "Kafka topic flush policies passed topic=%s defaults=%s%n", topic, DEFAULT);
    }

    private static void reject(
            Admin admin, ConfigResource resource, String name, String value) throws Exception {
        try {
            alter(admin, resource, List.of(set(name, value)));
            throw new AssertionError("invalid " + name + " was accepted");
        } catch (ExecutionException expected) {
            // The broker returns INVALID_REQUEST through the Admin future.
        }
    }

    private static AlterConfigOp set(String name, String value) {
        return new AlterConfigOp(new ConfigEntry(name, value), AlterConfigOp.OpType.SET);
    }

    private static AlterConfigOp delete(String name) {
        return new AlterConfigOp(new ConfigEntry(name, null), AlterConfigOp.OpType.DELETE);
    }

    private static void alter(
            Admin admin, ConfigResource resource, List<AlterConfigOp> operations) throws Exception {
        admin.incrementalAlterConfigs(Map.of(resource, operations))
                .all()
                .get(15, TimeUnit.SECONDS);
    }

    private static void assertValues(
            Admin admin,
            ConfigResource resource,
            String messages,
            String millis,
            ConfigEntry.ConfigSource source)
            throws Exception {
        Config config = admin.describeConfigs(
                        List.of(resource),
                        new DescribeConfigsOptions().includeSynonyms(true))
                .all()
                .get(15, TimeUnit.SECONDS)
                .get(resource);
        assertValue(config, MESSAGES, messages, source);
        assertValue(config, MILLIS, millis, source);
    }

    private static void assertValue(
            Config config, String name, String expected, ConfigEntry.ConfigSource source) {
        ConfigEntry entry = config.get(name);
        if (entry == null || !expected.equals(entry.value()) || entry.source() != source) {
            throw new AssertionError(
                    "unexpected "
                            + name
                            + ": "
                            + (entry == null
                                    ? null
                                    : entry.value() + " source=" + entry.source()));
        }
        int expectedSynonyms =
                source == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG ? 2 : 1;
        if (entry.synonyms().size() != expectedSynonyms) {
            throw new AssertionError(
                    "unexpected synonym count for " + name + ": " + entry.synonyms());
        }
        if (expectedSynonyms == 2) {
            ConfigEntry.ConfigSynonym dynamic = entry.synonyms().get(0);
            if (!name.equals(dynamic.name())
                    || !expected.equals(dynamic.value())
                    || dynamic.source() != source) {
                throw new AssertionError("unexpected dynamic synonym for " + name + ": " + dynamic);
            }
        }
        ConfigEntry.ConfigSynonym fallback =
                entry.synonyms().get(entry.synonyms().size() - 1);
        String fallbackName =
                MESSAGES.equals(name)
                        ? "log.flush.interval.messages"
                        : "log.flush.interval.ms";
        if (!fallbackName.equals(fallback.name())
                || !DEFAULT.equals(fallback.value())
                || fallback.source() != ConfigEntry.ConfigSource.DEFAULT_CONFIG) {
            throw new AssertionError("unexpected default synonym for " + name + ": " + fallback);
        }
    }
}
