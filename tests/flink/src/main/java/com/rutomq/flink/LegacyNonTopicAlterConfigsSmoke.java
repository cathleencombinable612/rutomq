package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterConfigsRequestData.AlterConfigsResource;
import org.apache.kafka.common.message.AlterConfigsRequestData.AlterableConfig;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AlterConfigsRequest;

public final class LegacyNonTopicAlterConfigsSmoke {
    private static final short VERSION = 2;
    private static final long TIMEOUT_SECONDS = 15;

    private LegacyNonTopicAlterConfigsSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "LegacyNonTopicAlterConfigsSmoke <bootstrap> <resource-prefix>");
        }
        String bootstrap = args[0];
        ConfigResource group =
                new ConfigResource(ConfigResource.Type.GROUP, args[1] + "-group");
        ConfigResource clientMetrics =
                new ConfigResource(ConfigResource.Type.CLIENT_METRICS, args[1] + "-client");
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(bootstrap))) {
            seed(admin, group, clientMetrics);
            replace(bootstrap, group, clientMetrics, true, 91001);
            assertSeeded(admin, group, clientMetrics);
            replace(bootstrap, group, clientMetrics, false, 91002);
            assertReplacement(admin, group, clientMetrics);
            clear(bootstrap, group, clientMetrics, 91003);
            assertCleared(admin, group, clientMetrics);
        }
        System.out.println(
                "Kafka 4.2 generated-wire legacy GROUP and CLIENT_METRICS replacement passed");
    }

    private static void seed(
            Admin admin, ConfigResource group, ConfigResource clientMetrics)
            throws Exception {
        Map<ConfigResource, Collection<AlterConfigOp>> changes =
                new LinkedHashMap<>();
        changes.put(
                group,
                List.of(
                        set("consumer.heartbeat.interval.ms", "5000"),
                        set("consumer.session.timeout.ms", "45000"),
                        set("streams.num.standby.replicas", "1")));
        changes.put(
                clientMetrics,
                List.of(
                        set("metrics", "*"),
                        set("interval.ms", "100"),
                        set("match", "client_id=legacy-.*")));
        admin.incrementalAlterConfigs(changes)
                .all()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static void replace(
            String bootstrap,
            ConfigResource group,
            ConfigResource clientMetrics,
            boolean validateOnly,
            int correlationId)
            throws Exception {
        AlterConfigsRequestData data =
                new AlterConfigsRequestData().setValidateOnly(validateOnly);
        data.resources().add(resource(
                group,
                List.of(config("consumer.session.timeout.ms", "60000"))));
        data.resources().add(resource(
                clientMetrics, List.of(config("metrics", "producer."))));
        assertSuccess(bootstrap, data, correlationId);
    }

    private static void clear(
            String bootstrap,
            ConfigResource group,
            ConfigResource clientMetrics,
            int correlationId)
            throws Exception {
        AlterConfigsRequestData data = new AlterConfigsRequestData();
        data.resources().add(resource(group, List.of()));
        data.resources().add(resource(clientMetrics, List.of()));
        assertSuccess(bootstrap, data, correlationId);
    }

    private static AlterConfigsResource resource(
            ConfigResource resource, List<AlterableConfig> configs) {
        AlterConfigsResource result = new AlterConfigsResource()
                .setResourceType(resource.type().id())
                .setResourceName(resource.name());
        result.configs().addAll(configs);
        return result;
    }

    private static AlterableConfig config(String name, String value) {
        return new AlterableConfig().setName(name).setValue(value);
    }

    private static void assertSuccess(
            String bootstrap, AlterConfigsRequestData data, int correlationId)
            throws Exception {
        ByteBuffer body = KafkaWire.exchange(
                bootstrap,
                new AlterConfigsRequest(data, VERSION),
                correlationId);
        AlterConfigsResponseData response =
                new AlterConfigsResponseData(new ByteBufferAccessor(body), VERSION);
        if (response.responses().size() != data.resources().size()
                || response.responses().stream()
                        .anyMatch(result -> result.errorCode() != Errors.NONE.code())) {
            throw new AssertionError("legacy AlterConfigs failed: " + response);
        }
    }

    private static void assertSeeded(
            Admin admin, ConfigResource group, ConfigResource clientMetrics)
            throws Exception {
        Config groupConfig = describe(admin, group);
        assertDynamic(groupConfig, "consumer.heartbeat.interval.ms", "5000");
        assertDynamic(groupConfig, "consumer.session.timeout.ms", "45000");
        assertDynamic(groupConfig, "streams.num.standby.replicas", "1");
        Config clientConfig = describe(admin, clientMetrics);
        assertDynamic(clientConfig, "metrics", "*");
        assertDynamic(clientConfig, "interval.ms", "100");
        assertDynamic(clientConfig, "match", "client_id=legacy-.*");
    }

    private static void assertReplacement(
            Admin admin, ConfigResource group, ConfigResource clientMetrics)
            throws Exception {
        Config groupConfig = describe(admin, group);
        assertDynamic(groupConfig, "consumer.session.timeout.ms", "60000");
        assertDefault(groupConfig, "consumer.heartbeat.interval.ms");
        assertDefault(groupConfig, "streams.num.standby.replicas");

        Config clientConfig = describe(admin, clientMetrics);
        assertDynamic(clientConfig, "metrics", "producer.");
        if (clientConfig.get("interval.ms") != null || clientConfig.get("match") != null) {
            throw new AssertionError(
                    "legacy ClientMetrics replacement retained omitted configs: "
                            + clientConfig.entries());
        }
    }

    private static void assertCleared(
            Admin admin, ConfigResource group, ConfigResource clientMetrics)
            throws Exception {
        Config groupConfig = describe(admin, group);
        assertDefault(groupConfig, "consumer.session.timeout.ms");
        assertDefault(groupConfig, "consumer.heartbeat.interval.ms");
        if (admin.listClientMetricsResources()
                .all()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .stream()
                .anyMatch(resource -> resource.name().equals(clientMetrics.name()))) {
            throw new AssertionError("empty legacy replacement retained ClientMetrics resource");
        }
    }

    private static Config describe(Admin admin, ConfigResource resource)
            throws Exception {
        return admin.describeConfigs(List.of(resource))
                .all()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .get(resource);
    }

    private static AlterConfigOp set(String name, String value) {
        return new AlterConfigOp(
                new ConfigEntry(name, value), AlterConfigOp.OpType.SET);
    }

    private static void assertDynamic(Config config, String name, String expected) {
        ConfigEntry entry = config.get(name);
        if (entry == null
                || !expected.equals(entry.value())
                || (entry.source() != ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG
                        && entry.source()
                                != ConfigEntry.ConfigSource.DYNAMIC_CLIENT_METRICS_CONFIG)) {
            throw new AssertionError(
                    "expected dynamic " + name + "=" + expected + ", got " + entry);
        }
    }

    private static void assertDefault(Config config, String name) {
        ConfigEntry entry = config.get(name);
        if (entry == null || entry.source() != ConfigEntry.ConfigSource.DEFAULT_CONFIG) {
            throw new AssertionError("expected default " + name + ", got " + entry);
        }
    }
}
