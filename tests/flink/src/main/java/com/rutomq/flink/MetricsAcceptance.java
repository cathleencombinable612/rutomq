package com.rutomq.flink;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;

final class MetricsAcceptance {
    private MetricsAcceptance() {}

    static void awaitConsumerSnapshot(String metricsUrl, String group, String topic)
            throws Exception {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        String body = "";
        while (System.nanoTime() < deadline) {
            body = client.send(
                            HttpRequest.newBuilder(URI.create(metricsUrl))
                                    .timeout(Duration.ofSeconds(5))
                                    .GET()
                                    .build(),
                            HttpResponse.BodyHandlers.ofString())
                    .body();
            if (value(body, "rutomq_group_members",
                            "protocol=\"consumer\"", "group=\"" + group + "\"",
                            "state=\"Stable\"")
                            == 2
                    && value(body, "rutomq_group_rebalance_epoch",
                                    "protocol=\"consumer\"", "group=\"" + group + "\"")
                            > 0
                    && zeroLagPartitions(body, group, topic) == 2
                    && zeroRetentionPartitions(body, topic) == 2
                    && value(body, "rutomq_transactions_by_state",
                                    "state=\"CompleteCommit\"")
                            > 0) {
                return;
            }
            Thread.sleep(250);
        }
        throw new AssertionError("control-plane metrics did not converge:\n" + relevant(body));
    }

    private static long zeroLagPartitions(String body, String group, String topic) {
        return body.lines()
                .filter(line -> matches(
                        line,
                        "rutomq_consumer_group_lag",
                        "group=\"" + group + "\"",
                        "topic=\"" + topic + "\""))
                .filter(line -> metricValue(line) == 0)
                .count();
    }

    private static long zeroRetentionPartitions(String body, String topic) {
        return body.lines()
                .filter(line -> matches(
                        line,
                        "rutomq_partition_retention_size_percent",
                        "topic=\"" + topic + "\""))
                .filter(line -> metricValue(line) == 0)
                .count();
    }

    private static long value(String body, String name, String... labels) {
        return body.lines()
                .filter(line -> matches(line, name, labels))
                .mapToLong(MetricsAcceptance::metricValue)
                .findFirst()
                .orElse(Long.MIN_VALUE);
    }

    private static boolean matches(String line, String name, String... labels) {
        return line.startsWith(name + "{")
                && Arrays.stream(labels).allMatch(line::contains);
    }

    private static long metricValue(String line) {
        int separator = line.lastIndexOf(' ');
        return separator < 0
                ? Long.MIN_VALUE
                : Math.round(Double.parseDouble(line.substring(separator + 1)));
    }

    private static String relevant(String body) {
        return body.lines()
                .filter(line -> line.startsWith("rutomq_group_")
                        || line.startsWith("rutomq_consumer_group_lag")
                        || line.startsWith("rutomq_partition_retention_size")
                        || line.startsWith("rutomq_transactions_by_state")
                        || line.startsWith("rutomq_observability_collection_errors"))
                .reduce("", (left, right) -> left + right + "\n");
    }
}
