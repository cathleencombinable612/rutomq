package com.rutomq.kafka3;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;

public final class LegacyClientMetricsSmoke {
    private static final long TIMEOUT_SECONDS = 15;

    private LegacyClientMetricsSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "LegacyClientMetricsSmoke <bootstrap> <resource-name>");
        }
        ConfigResource resource =
                new ConfigResource(ConfigResource.Type.CLIENT_METRICS, args[1]);
        try (Admin admin = Admin.create(properties(args[0]))) {
            admin.incrementalAlterConfigs(Map.of(
                            resource,
                            List.of(
                                    set("metrics", "*"),
                                    set("interval.ms", "100"),
                                    set("match", "client_id=kafka3-.*"))))
                    .all()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            Config replacement =
                    new Config(List.of(new ConfigEntry("metrics", "producer.")));
            admin.alterConfigs(
                            Map.of(resource, replacement),
                            new AlterConfigsOptions().validateOnly(true))
                    .all()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertConfig(admin, resource, "metrics", "*");
            assertConfig(admin, resource, "interval.ms", "100");
            assertConfig(admin, resource, "match", "client_id=kafka3-.*");

            admin.alterConfigs(Map.of(resource, replacement))
                    .all()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Config replaced = describe(admin, resource);
            assertConfig(replaced, "metrics", "producer.");
            if (replaced.get("interval.ms") != null || replaced.get("match") != null) {
                throw new AssertionError(
                        "Kafka 3.9 legacy replacement retained omitted configs: "
                                + replaced.entries());
            }

            admin.alterConfigs(Map.of(resource, new Config(List.of())))
                    .all()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (admin.listClientMetricsResources()
                    .all()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .stream()
                    .anyMatch(listing -> listing.name().equals(resource.name()))) {
                throw new AssertionError(
                        "Kafka 3.9 empty legacy replacement retained the resource");
            }
        }
        System.out.println(
                "Kafka 3.9 AdminClient legacy CLIENT_METRICS replacement passed");
    }

    private static AlterConfigOp set(String name, String value) {
        return new AlterConfigOp(
                new ConfigEntry(name, value), AlterConfigOp.OpType.SET);
    }

    private static Config describe(Admin admin, ConfigResource resource)
            throws Exception {
        return admin.describeConfigs(List.of(resource))
                .all()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .get(resource);
    }

    private static void assertConfig(
            Admin admin, ConfigResource resource, String name, String expected)
            throws Exception {
        assertConfig(describe(admin, resource), name, expected);
    }

    private static void assertConfig(Config config, String name, String expected) {
        ConfigEntry entry = config.get(name);
        if (entry == null
                || !expected.equals(entry.value())
                || entry.source()
                        != ConfigEntry.ConfigSource.DYNAMIC_CLIENT_METRICS_CONFIG) {
            throw new AssertionError(
                    "unexpected ClientMetrics config " + name + ": " + entry);
        }
    }

    private static Properties properties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "rutomq-kafka3-legacy-admin");
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        return properties;
    }
}
