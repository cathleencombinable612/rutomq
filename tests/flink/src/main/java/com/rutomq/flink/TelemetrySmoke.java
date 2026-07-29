package com.rutomq.flink;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ClientMetricsResourceListing;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;

public final class TelemetrySmoke {
    private static final String RESOURCE_NAME = "rutomq-java-telemetry";
    private static final ConfigResource RESOURCE =
            new ConfigResource(ConfigResource.Type.CLIENT_METRICS, RESOURCE_NAME);

    private TelemetrySmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("TelemetrySmoke <bootstrap> <metrics-url>");
        }
        String bootstrap = args[0];
        try (Admin admin = Admin.create(properties(bootstrap, "telemetry-config-admin"))) {
            alter(
                    admin,
                    List.of(
                            set("metrics", "*"),
                            set("interval.ms", "100"),
                            set("match", "client_id=telemetry-client")));
            Collection<ClientMetricsResourceListing> resources =
                    admin.listClientMetricsResources().all().get(10, TimeUnit.SECONDS);
            if (resources.stream().noneMatch(resource -> RESOURCE_NAME.equals(resource.name()))) {
                throw new AssertionError("client metrics resource was not listed");
            }
            Config config =
                    admin.describeConfigs(
                                    List.of(RESOURCE),
                                    new DescribeConfigsOptions().includeSynonyms(true))
                            .all()
                            .get(10, TimeUnit.SECONDS)
                            .get(RESOURCE);
            assertConfig(config, "metrics", "*");
            assertConfig(config, "interval.ms", "100");
            assertConfig(config, "match", "client_id=telemetry-client");
        }

        try (Admin telemetryClient = Admin.create(properties(bootstrap, "telemetry-client"))) {
            Uuid clientInstanceId = telemetryClient.clientInstanceId(Duration.ofSeconds(10));
            if (clientInstanceId == null || Uuid.ZERO_UUID.equals(clientInstanceId)) {
                throw new AssertionError("broker did not assign a telemetry client instance id");
            }
            awaitPush(args[1]);
        }

        try (Admin admin = Admin.create(properties(bootstrap, "telemetry-cleanup-admin"))) {
            alter(
                    admin,
                    List.of(delete("metrics"), delete("interval.ms"), delete("match")));
            Collection<ClientMetricsResourceListing> resources =
                    admin.listClientMetricsResources().all().get(10, TimeUnit.SECONDS);
            if (resources.stream().anyMatch(resource -> RESOURCE_NAME.equals(resource.name()))) {
                throw new AssertionError("client metrics resource was not removed");
            }
        }
        System.out.println("Kafka client telemetry config/get/push acceptance passed");
    }

    private static void alter(Admin admin, List<AlterConfigOp> operations) throws Exception {
        admin.incrementalAlterConfigs(Map.of(RESOURCE, operations))
                .all()
                .get(10, TimeUnit.SECONDS);
    }

    private static AlterConfigOp set(String name, String value) {
        return new AlterConfigOp(
                new ConfigEntry(name, value), AlterConfigOp.OpType.SET);
    }

    private static AlterConfigOp delete(String name) {
        return new AlterConfigOp(
                new ConfigEntry(name, null), AlterConfigOp.OpType.DELETE);
    }

    private static void assertConfig(Config config, String name, String expected) {
        ConfigEntry entry = config.get(name);
        if (entry == null
                || !expected.equals(entry.value())
                || entry.source()
                        != ConfigEntry.ConfigSource.DYNAMIC_CLIENT_METRICS_CONFIG
                || entry.synonyms().size() != 1
                || !name.equals(entry.synonyms().get(0).name())
                || !expected.equals(entry.synonyms().get(0).value())
                || entry.synonyms().get(0).source()
                        != ConfigEntry.ConfigSource.DYNAMIC_CLIENT_METRICS_CONFIG) {
            throw new AssertionError("unexpected client metrics config " + name + ": " + entry);
        }
    }

    private static void awaitPush(String metricsUrl) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create(metricsUrl)).GET().build();
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200
                    && counter(response.body(), "rutomq_client_telemetry_pushes_total") > 0) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Kafka client did not push telemetry before the deadline");
    }

    private static double counter(String metrics, String name) {
        for (String line : metrics.split("\\R")) {
            if (line.startsWith(name + " ")) {
                return Double.parseDouble(line.substring(name.length()).trim());
            }
        }
        return 0;
    }

    private static Properties properties(String bootstrap, String clientId) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        properties.put(CommonClientConfigs.ENABLE_METRICS_PUSH_CONFIG, "true");
        return properties;
    }
}
