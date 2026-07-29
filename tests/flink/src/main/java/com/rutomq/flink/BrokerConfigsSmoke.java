package com.rutomq.flink;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource;
import org.apache.kafka.clients.admin.ConfigEntry.ConfigType;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.ListConfigResourcesOptions;
import org.apache.kafka.common.config.ConfigResource;

public final class BrokerConfigsSmoke {
    private static final String GROUP_BUFFER =
            "group.coordinator.cached.buffer.max.bytes";
    private static final String SHARE_BUFFER =
            "share.coordinator.cached.buffer.max.bytes";
    private static final String REBALANCE_PROTOCOLS =
            "group.coordinator.rebalance.protocols";
    private static final String TWO_PHASE_COMMIT =
            "transaction.two.phase.commit.enable";
    private static final String TRANSACTION_MAX_TIMEOUT =
            "transaction.max.timeout.ms";
    private static final String TRANSACTIONAL_ID_EXPIRATION =
            "transactional.id.expiration.ms";
    private static final String TRANSACTIONAL_ID_CLEANUP_INTERVAL =
            "transaction.remove.expired.transaction.cleanup.interval.ms";
    private static final String TRANSACTION_ABORT_CLEANUP_INTERVAL =
            "transaction.abort.timed.out.transaction.cleanup.interval.ms";
    private static final String TRANSACTION_PARTITION_VERIFICATION =
            "transaction.partition.verification.enable";

    private BrokerConfigsSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("BrokerConfigsSmoke <bootstrap>");
        }
        ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER, "0");
        ConfigResource brokerDefault =
                new ConfigResource(ConfigResource.Type.BROKER, "");
        ConfigResource logger =
                new ConfigResource(ConfigResource.Type.BROKER_LOGGER, "0");
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(args[0]))) {
            var listed = admin.listConfigResources(
                            Set.of(
                                    ConfigResource.Type.BROKER,
                                    ConfigResource.Type.BROKER_LOGGER),
                            new ListConfigResourcesOptions())
                    .all()
                    .get(15, TimeUnit.SECONDS);
            if (listed.size() != 2 || !listed.containsAll(Set.of(broker, logger))) {
                throw new AssertionError("unexpected broker config resources " + listed);
            }

            Map<ConfigResource, Config> described = admin.describeConfigs(
                            List.of(broker, brokerDefault, logger),
                            new DescribeConfigsOptions()
                                    .includeSynonyms(true)
                                    .includeDocumentation(true))
                    .all()
                    .get(15, TimeUnit.SECONDS);
            assertBroker(described.get(broker));
            assertDefaultBroker(described.get(brokerDefault));
            assertLogger(described.get(logger));

            admin.incrementalAlterConfigs(Map.of(
                            brokerDefault,
                            List.of(
                                    set(GROUP_BUFFER, "524288"),
                                    set(SHARE_BUFFER, "2097152"),
                                    set(TRANSACTION_PARTITION_VERIFICATION, "false"))))
                    .all()
                    .get(15, TimeUnit.SECONDS);
            Config dynamic = admin.describeConfigs(
                            List.of(brokerDefault),
                            new DescribeConfigsOptions()
                                    .includeSynonyms(true)
                                    .includeDocumentation(true))
                    .all()
                    .get(15, TimeUnit.SECONDS)
                    .get(brokerDefault);
            assertDynamicBuffer(dynamic, GROUP_BUFFER, "524288");
            assertDynamicBuffer(dynamic, SHARE_BUFFER, "2097152");
            assertDynamicBoolean(
                    dynamic, TRANSACTION_PARTITION_VERIFICATION, "false");
            admin.incrementalAlterConfigs(Map.of(
                            brokerDefault,
                            List.of(
                                    delete(GROUP_BUFFER),
                                    delete(SHARE_BUFFER),
                                    delete(TRANSACTION_PARTITION_VERIFICATION))))
                    .all()
                    .get(15, TimeUnit.SECONDS);
        }
        System.out.println(
                "Kafka 4.3 virtual broker, transaction expiration and partition verification, KIP-1196 buffers, KIP-1237 protocols, KIP-939 gate, assignment defaults, and logger configs passed");
    }

    private static void assertBroker(Config config) {
        ConfigEntry nodeId = required(config, "node.id");
        if (!"0".equals(nodeId.value())
                || nodeId.source() != ConfigSource.STATIC_BROKER_CONFIG
                || !nodeId.isReadOnly()
                || nodeId.type() != ConfigType.INT
                || nodeId.synonyms().size() != 1
                || nodeId.documentation() == null
                || nodeId.documentation().isBlank()) {
            throw new AssertionError("unexpected node.id config " + nodeId);
        }
        ConfigEntry listener = required(config, "advertised.listeners");
        if (!listener.value().contains("rutomq:9092")) {
            throw new AssertionError(
                    "unexpected advertised listener " + listener.value());
        }
        ConfigEntry noWal = required(config, "rutomq.local.wal.enabled");
        if (!"false".equals(noWal.value()) || noWal.type() != ConfigType.BOOLEAN) {
            throw new AssertionError("unexpected WAL config " + noWal);
        }
        ConfigEntry partitionLimit =
                required(config, "max.request.partition.size.limit");
        if (!"2".equals(partitionLimit.value())
                || partitionLimit.type() != ConfigType.INT) {
            throw new AssertionError(
                    "unexpected partition response limit " + partitionLimit);
        }
        ConfigEntry backgroundThreads =
                required(config, "group.coordinator.background.threads");
        if (!"2".equals(backgroundThreads.value())
                || !backgroundThreads.isReadOnly()
                || backgroundThreads.type() != ConfigType.INT) {
            throw new AssertionError(
                    "unexpected coordinator background threads "
                            + backgroundThreads);
        }
        ConfigEntry rebalanceProtocols = required(config, REBALANCE_PROTOCOLS);
        if (!"classic,consumer,streams".equals(rebalanceProtocols.value())
                || rebalanceProtocols.source() != ConfigSource.STATIC_BROKER_CONFIG
                || !rebalanceProtocols.isReadOnly()
                || rebalanceProtocols.type() != ConfigType.LIST
                || rebalanceProtocols.synonyms().size() != 1
                || rebalanceProtocols.documentation() == null
                || rebalanceProtocols.documentation().isBlank()) {
            throw new AssertionError(
                    "unexpected rebalance protocol config "
                            + rebalanceProtocols);
        }
        ConfigEntry twoPhaseCommit = required(config, TWO_PHASE_COMMIT);
        if (!"true".equals(twoPhaseCommit.value())
                || twoPhaseCommit.source() != ConfigSource.STATIC_BROKER_CONFIG
                || !twoPhaseCommit.isReadOnly()
                || twoPhaseCommit.type() != ConfigType.BOOLEAN
                || twoPhaseCommit.synonyms().size() != 1
                || twoPhaseCommit.documentation() == null
                || twoPhaseCommit.documentation().isBlank()) {
            throw new AssertionError(
                    "unexpected two-phase transaction config "
                            + twoPhaseCommit);
        }
        ConfigEntry transactionMaxTimeout =
                required(config, TRANSACTION_MAX_TIMEOUT);
        if (!"900000".equals(transactionMaxTimeout.value())
                || transactionMaxTimeout.source()
                        != ConfigSource.STATIC_BROKER_CONFIG
                || !transactionMaxTimeout.isReadOnly()
                || transactionMaxTimeout.type() != ConfigType.INT
                || transactionMaxTimeout.synonyms().size() != 1
                || transactionMaxTimeout.documentation() == null
                || transactionMaxTimeout.documentation().isBlank()) {
            throw new AssertionError(
                    "unexpected transaction timeout config "
                            + transactionMaxTimeout);
        }
        assertStaticInt(
                config,
                TRANSACTIONAL_ID_EXPIRATION,
                "604800000");
        assertStaticInt(
                config,
                TRANSACTIONAL_ID_CLEANUP_INTERVAL,
                "3600000");
        assertStaticInt(
                config,
                TRANSACTION_ABORT_CLEANUP_INTERVAL,
                "10000");
        assertStaticDynamicBoolean(
                config, TRANSACTION_PARTITION_VERIFICATION, "true");
        if (config.get("log.dirs") != null || config.get("replica.fetch.max.bytes") != null) {
            throw new AssertionError("physical Kafka storage configs were exposed");
        }
    }

    private static void assertStaticInt(
            Config config, String name, String expectedValue) {
        ConfigEntry entry = required(config, name);
        if (!expectedValue.equals(entry.value())
                || entry.source() != ConfigSource.STATIC_BROKER_CONFIG
                || !entry.isReadOnly()
                || entry.type() != ConfigType.INT
                || entry.synonyms().size() != 1
                || entry.documentation() == null
                || entry.documentation().isBlank()) {
            throw new AssertionError("unexpected static int config " + entry);
        }
    }

    private static void assertLogger(Config config) {
        ConfigEntry filter = required(config, "rutomq.tracing.filter");
        if (!"rutomq=debug".equals(filter.value())
                || filter.source() != ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG
                || !filter.isReadOnly()
                || filter.type() != ConfigType.STRING
                || filter.synonyms().size() != 1
                || filter.documentation() == null
                || filter.documentation().isBlank()) {
            throw new AssertionError("unexpected tracing filter " + filter);
        }
    }

    private static void assertDefaultBroker(Config config) {
        List<String> intervalNames = List.of(
                "group.consumer.assignment.interval.ms",
                "group.share.assignment.interval.ms",
                "group.streams.assignment.interval.ms");
        List<String> offloadNames = List.of(
                "group.consumer.assignor.offload.enable",
                "group.share.assignor.offload.enable",
                "group.streams.assignor.offload.enable");
        List<String> bufferNames = List.of(GROUP_BUFFER, SHARE_BUFFER);
        if (config.entries().size()
                != intervalNames.size() + offloadNames.size()
                        + bufferNames.size() + 1) {
            throw new AssertionError(
                    "unexpected cluster-wide broker defaults " + config);
        }
        for (String name : intervalNames) {
            ConfigEntry entry = required(config, name);
            if (!"1000".equals(entry.value())
                    || entry.source() != ConfigSource.STATIC_BROKER_CONFIG
                    || entry.isReadOnly()
                    || entry.type() != ConfigType.INT
                    || entry.synonyms().size() != 1
                    || entry.documentation() == null
                    || entry.documentation().isBlank()) {
                throw new AssertionError(
                        "unexpected assignment default " + entry);
            }
        }
        for (String name : offloadNames) {
            ConfigEntry entry = required(config, name);
            if (!"true".equals(entry.value())
                    || entry.source() != ConfigSource.STATIC_BROKER_CONFIG
                    || entry.isReadOnly()
                    || entry.type() != ConfigType.BOOLEAN
                    || entry.synonyms().size() != 1
                    || entry.documentation() == null
                    || entry.documentation().isBlank()) {
                throw new AssertionError(
                        "unexpected assignment offload default " + entry);
            }
        }
        for (String name : bufferNames) {
            ConfigEntry entry = required(config, name);
            if (!"1048588".equals(entry.value())
                    || entry.source() != ConfigSource.STATIC_BROKER_CONFIG
                    || entry.isReadOnly()
                    || entry.type() != ConfigType.INT
                    || entry.synonyms().size() != 1
                    || entry.documentation() == null
                    || entry.documentation().isBlank()) {
                throw new AssertionError(
                        "unexpected coordinator buffer default " + entry);
            }
        }
        assertStaticDynamicBoolean(
                config, TRANSACTION_PARTITION_VERIFICATION, "true");
    }

    private static void assertDynamicBuffer(Config config, String name, String value) {
        ConfigEntry entry = required(config, name);
        if (!value.equals(entry.value())
                || entry.source() != ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG
                || entry.isReadOnly()
                || entry.type() != ConfigType.INT
                || entry.synonyms().size() != 2) {
            throw new AssertionError("unexpected dynamic coordinator buffer " + entry);
        }
    }

    private static void assertStaticDynamicBoolean(
            Config config, String name, String expectedValue) {
        ConfigEntry entry = required(config, name);
        if (!expectedValue.equals(entry.value())
                || entry.source() != ConfigSource.STATIC_BROKER_CONFIG
                || entry.isReadOnly()
                || entry.type() != ConfigType.BOOLEAN
                || entry.synonyms().size() != 1
                || entry.documentation() == null
                || entry.documentation().isBlank()) {
            throw new AssertionError("unexpected dynamic boolean default " + entry);
        }
    }

    private static void assertDynamicBoolean(
            Config config, String name, String expectedValue) {
        ConfigEntry entry = required(config, name);
        if (!expectedValue.equals(entry.value())
                || entry.source() != ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG
                || entry.isReadOnly()
                || entry.type() != ConfigType.BOOLEAN
                || entry.synonyms().size() != 2) {
            throw new AssertionError("unexpected dynamic boolean config " + entry);
        }
    }

    private static AlterConfigOp set(String name, String value) {
        return new AlterConfigOp(
                new ConfigEntry(name, value), AlterConfigOp.OpType.SET);
    }

    private static AlterConfigOp delete(String name) {
        return new AlterConfigOp(
                new ConfigEntry(name, null), AlterConfigOp.OpType.DELETE);
    }

    private static ConfigEntry required(Config config, String name) {
        ConfigEntry entry = config.get(name);
        if (entry == null) {
            throw new AssertionError("missing config " + name + " in " + config);
        }
        return entry;
    }
}
