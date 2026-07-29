package com.rutomq.security;

import java.nio.file.Files;
import java.nio.file.Path;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateDelegationTokenOptions;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.ExpireDelegationTokenOptions;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RenewDelegationTokenOptions;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.SupportedVersionRange;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.clients.admin.UserScramCredentialDeletion;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidUpdateVersionException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedByAuthenticationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsPartition;
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsTopic;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData;
import org.apache.kafka.common.message.DescribeLogDirsRequestData;
import org.apache.kafka.common.message.ExpireDelegationTokenRequestData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestPartition;
import org.apache.kafka.common.message.OffsetDeleteRequestData.OffsetDeleteRequestTopic;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RenewDelegationTokenRequestData;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DeleteRecordsRequest;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.requests.CreateDelegationTokenRequest;
import org.apache.kafka.common.requests.CreateDelegationTokenResponse;
import org.apache.kafka.common.requests.DescribeLogDirsRequest;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.apache.kafka.common.requests.ExpireDelegationTokenRequest;
import org.apache.kafka.common.requests.ExpireDelegationTokenResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.OffsetDeleteRequest;
import org.apache.kafka.common.requests.OffsetDeleteResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RenewDelegationTokenRequest;
import org.apache.kafka.common.requests.RenewDelegationTokenResponse;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

public final class KafkaSecuritySmoke {
    private KafkaSecuritySmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 6 || args.length > 7) {
            throw new IllegalArgumentException(
                    "usage: KafkaSecuritySmoke <bootstrap> <certificate> "
                            + "<mechanism> <username> <password> <topic> [provision-acls]");
        }
        String bootstrap = args[0];
        String certificate = Files.readString(Path.of(args[1]));
        String mechanism = args[2];
        String username = args[3];
        String password = args[4];
        String topic = args[5];
        String action = args.length == 7 ? args[6] : "smoke";
        Properties common = securityProperties(
                bootstrap, certificate, mechanism, username, password);

        if ("provision-acls".equals(action)) {
            provisionAcls(common, topic);
            System.out.printf("%s ACLs provisioned%n", topic);
            return;
        }
        if ("delegation-token".equals(action)) {
            delegationTokenSmoke(common, mechanism, topic);
            return;
        }
        if ("delegation-token-requester".equals(action)) {
            delegationTokenRequesterSmoke(common, mechanism, topic);
            return;
        }
        if ("authorization-filtering".equals(action)) {
            authorizationFiltering(common, topic);
            return;
        }
        if ("describe-log-dirs-v5".equals(action)) {
            describeLogDirsV5(common);
            return;
        }
        if ("metadata-version-43".equals(action)) {
            metadataVersion43(common);
            return;
        }
        if ("kip-1240-group-configs".equals(action)) {
            kip1240GroupConfigs(common, topic);
            return;
        }
        if ("kip-1263-assignment-intervals".equals(action)) {
            kip1263AssignmentIntervals(common, topic);
            return;
        }
        if ("kip-1196-buffer-controls".equals(action)) {
            kip1196BufferControls(common);
            return;
        }
        if (!"smoke".equals(action)) {
            throw new IllegalArgumentException("unknown action " + action);
        }
        createTopic(common, topic);
        produce(common, topic);
        consume(common, topic);
        System.out.printf("%s SASL_SSL smoke passed%n", mechanism);
    }

    private static void metadataVersion43(Properties properties)
            throws Exception {
        try (Admin admin = Admin.create(properties)) {
            FeatureMetadata initial = admin.describeFeatures()
                    .featureMetadata()
                    .get(10, TimeUnit.SECONDS);
            SupportedVersionRange supported =
                    initial.supportedFeatures().get("metadata.version");
            if (supported == null
                    || supported.minVersion() != 22
                    || supported.maxVersion() != 30) {
                throw new AssertionError(
                        "unexpected Kafka 4.3 metadata.version support "
                                + supported);
            }
            assertFinalizedMetadataVersion(initial, (short) 29);
            long initialEpoch = initial.finalizedFeaturesEpoch().orElseThrow();

            admin.updateFeatures(
                            Map.of(
                                    "metadata.version",
                                    new FeatureUpdate(
                                            (short) 30,
                                            FeatureUpdate.UpgradeType.UPGRADE)),
                            new UpdateFeaturesOptions())
                    .all()
                    .get(10, TimeUnit.SECONDS);
            FeatureMetadata upgraded = admin.describeFeatures()
                    .featureMetadata()
                    .get(10, TimeUnit.SECONDS);
            assertFinalizedMetadataVersion(upgraded, (short) 30);
            if (upgraded.finalizedFeaturesEpoch().orElseThrow()
                    != initialEpoch + 1) {
                throw new AssertionError(
                        "metadata.version upgrade did not advance feature epoch");
            }

            for (FeatureUpdate.UpgradeType downgradeType :
                    List.of(
                            FeatureUpdate.UpgradeType.SAFE_DOWNGRADE,
                            FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE)) {
                try {
                    admin.updateFeatures(
                                    Map.of(
                                            "metadata.version",
                                            new FeatureUpdate(
                                                    (short) 29,
                                                    downgradeType)),
                                    new UpdateFeaturesOptions())
                            .all()
                            .get(10, TimeUnit.SECONDS);
                    throw new AssertionError(
                            "Kafka 4.3 metadata downgrade was accepted: "
                                    + downgradeType);
                } catch (ExecutionException error) {
                    if (!(error.getCause()
                            instanceof InvalidUpdateVersionException)) {
                        throw error;
                    }
                }
                FeatureMetadata unchanged = admin.describeFeatures()
                        .featureMetadata()
                        .get(10, TimeUnit.SECONDS);
                assertFinalizedMetadataVersion(unchanged, (short) 30);
                if (unchanged.finalizedFeaturesEpoch().orElseThrow()
                        != initialEpoch + 1) {
                    throw new AssertionError(
                            "rejected metadata downgrade changed feature epoch");
                }
            }
        }
        System.out.println(
                "Kafka 4.3 metadata.version level 30 negotiation passed");
    }

    private static void assertFinalizedMetadataVersion(
            FeatureMetadata metadata, short expected) {
        FinalizedVersionRange finalized =
                metadata.finalizedFeatures().get("metadata.version");
        if (finalized == null
                || finalized.minVersionLevel() != expected
                || finalized.maxVersionLevel() != expected) {
            throw new AssertionError(
                    "unexpected finalized metadata.version " + finalized);
        }
    }

    private static void describeLogDirsV5(Properties properties)
            throws Exception {
        AdminClientConfig config = new AdminClientConfig(properties);
        Time time = Time.SYSTEM;
        Metrics metrics = new Metrics();
        String bootstrap = properties
                .getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)
                .split(",", 2)[0];
        int separator = bootstrap.lastIndexOf(':');
        String host = bootstrap.substring(0, separator);
        int port = Integer.parseInt(bootstrap.substring(separator + 1));
        Node node = new Node(0, host, port);
        LogContext logContext = new LogContext("[describe-log-dirs-v5] ");
        Metadata metadata = new Metadata(
                config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG),
                config.getLong(AdminClientConfig.RETRY_BACKOFF_MAX_MS_CONFIG),
                config.getLong(AdminClientConfig.METADATA_MAX_AGE_CONFIG),
                logContext,
                new ClusterResourceListeners());
        metadata.bootstrap(List.of(new InetSocketAddress(host, port)));
        NetworkClient client = ClientUtils.createNetworkClient(
                config,
                metrics,
                "describe-log-dirs-v5",
                logContext,
                new ApiVersions(),
                time,
                1,
                metadata,
                null,
                null);
        try {
            if (!NetworkClientUtils.awaitReady(client, node, time, 10_000)) {
                throw new AssertionError(
                        "DescribeLogDirs v5 client was not ready");
            }
            ClientRequest request = client.newClientRequest(
                    node.idString(),
                    new DescribeLogDirsRequest.Builder(
                            new DescribeLogDirsRequestData().setTopics(null)),
                    time.milliseconds(),
                    true);
            ClientResponse response =
                    NetworkClientUtils.sendAndReceive(client, request, time);
            DescribeLogDirsResponse logDirs =
                    (DescribeLogDirsResponse) response.responseBody();
            if (response.requestHeader().apiVersion() != 5
                    || logDirs.data().errorCode() != Errors.NONE.code()
                    || logDirs.data().results().size() != 1) {
                throw new AssertionError(
                        "DescribeLogDirs v5 negotiation failed: version="
                                + response.requestHeader().apiVersion()
                                + " response="
                                + logDirs.data());
            }
            var directory = logDirs.data().results().get(0);
            if (!"/rutomq/object-store".equals(directory.logDir())
                    || directory.totalBytes() != -1
                    || directory.usableBytes() != -1
                    || directory.isCordoned()) {
                throw new AssertionError(
                        "unexpected virtual object-store directory "
                                + directory);
            }
        } finally {
            client.close();
            metrics.close();
        }
        System.out.println(
                "Kafka 4.3 DescribeLogDirs v5 negotiation and virtual directory passed");
    }

    private static void kip1240GroupConfigs(Properties properties, String prefix)
            throws Exception {
        ConfigResource group =
                new ConfigResource(ConfigResource.Type.GROUP, prefix + "-kip1240-group");
        Map<ConfigResource, Collection<AlterConfigOp>> changes = Map.of(
                group,
                List.of(
                        new AlterConfigOp(
                                new ConfigEntry("share.delivery.count.limit", "3"),
                                AlterConfigOp.OpType.SET),
                        new AlterConfigOp(
                                new ConfigEntry(
                                        "share.partition.max.record.locks", "123"),
                                AlterConfigOp.OpType.SET),
                        new AlterConfigOp(
                                new ConfigEntry(
                                        "share.renew.acknowledge.enable", "false"),
                                AlterConfigOp.OpType.SET)));
        try (Admin admin = Admin.create(properties)) {
            admin.incrementalAlterConfigs(changes).all().get(10, TimeUnit.SECONDS);
            Map<String, ConfigEntry> entries = admin.describeConfigs(List.of(group))
                    .all()
                    .get(10, TimeUnit.SECONDS)
                    .get(group)
                    .entries()
                    .stream()
                    .collect(java.util.stream.Collectors.toMap(
                            ConfigEntry::name, entry -> entry));
            assertConfig(entries, "share.delivery.count.limit", "3");
            assertConfig(entries, "share.partition.max.record.locks", "123");
            assertConfig(entries, "share.renew.acknowledge.enable", "false");

            Map<ConfigResource, Collection<AlterConfigOp>> invalid = Map.of(
                    group,
                    List.of(new AlterConfigOp(
                            new ConfigEntry("share.delivery.count.limit", "1"),
                            AlterConfigOp.OpType.SET)));
            try {
                admin.incrementalAlterConfigs(invalid).all().get(10, TimeUnit.SECONDS);
                throw new AssertionError(
                        "out-of-bounds share delivery count was accepted");
            } catch (ExecutionException error) {
                if (!(error.getCause() instanceof InvalidRequestException)) {
                    throw error;
                }
            }
        }
        System.out.println(
                "Kafka 4.3 KIP-1240 share group configurations passed");
    }

    private static void kip1263AssignmentIntervals(
            Properties properties, String prefix) throws Exception {
        ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER, "");
        ConfigResource group =
                new ConfigResource(ConfigResource.Type.GROUP, prefix + "-kip1263-group");
        List<String> brokerKeys = List.of(
                "group.consumer.assignment.interval.ms",
                "group.share.assignment.interval.ms",
                "group.streams.assignment.interval.ms");
        List<String> groupKeys = List.of(
                "consumer.assignment.interval.ms",
                "share.assignment.interval.ms",
                "streams.assignment.interval.ms");
        Map<ConfigResource, Collection<AlterConfigOp>> brokerChanges = Map.of(
                broker,
                List.of(
                        new AlterConfigOp(
                                new ConfigEntry(brokerKeys.get(0), "100"),
                                AlterConfigOp.OpType.SET),
                        new AlterConfigOp(
                                new ConfigEntry(brokerKeys.get(1), "200"),
                                AlterConfigOp.OpType.SET),
                        new AlterConfigOp(
                                new ConfigEntry(brokerKeys.get(2), "300"),
                                AlterConfigOp.OpType.SET)));
        Map<ConfigResource, Collection<AlterConfigOp>> groupChanges = Map.of(
                group,
                List.of(
                        new AlterConfigOp(
                                new ConfigEntry(groupKeys.get(0), "400"),
                                AlterConfigOp.OpType.SET),
                        new AlterConfigOp(
                                new ConfigEntry(groupKeys.get(1), "500"),
                                AlterConfigOp.OpType.SET),
                        new AlterConfigOp(
                                new ConfigEntry(groupKeys.get(2), "600"),
                                AlterConfigOp.OpType.SET)));
        try (Admin admin = Admin.create(properties)) {
            admin.incrementalAlterConfigs(brokerChanges).all().get(10, TimeUnit.SECONDS);
            admin.incrementalAlterConfigs(groupChanges).all().get(10, TimeUnit.SECONDS);
            Map<ConfigResource, org.apache.kafka.clients.admin.Config> described =
                    admin.describeConfigs(List.of(broker, group))
                            .all()
                            .get(10, TimeUnit.SECONDS);
            Map<String, ConfigEntry> brokerEntries = described.get(broker)
                    .entries()
                    .stream()
                    .collect(java.util.stream.Collectors.toMap(
                            ConfigEntry::name, entry -> entry));
            Map<String, ConfigEntry> groupEntries = described.get(group)
                    .entries()
                    .stream()
                    .collect(java.util.stream.Collectors.toMap(
                            ConfigEntry::name, entry -> entry));
            for (int index = 0; index < brokerKeys.size(); index++) {
                assertConfig(
                        brokerEntries,
                        brokerKeys.get(index),
                        Integer.toString((index + 1) * 100),
                        ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG);
                assertConfig(
                        groupEntries,
                        groupKeys.get(index),
                        Integer.toString((index + 4) * 100),
                        ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG);
            }

            Map<ConfigResource, Collection<AlterConfigOp>> invalid = Map.of(
                    group,
                    List.of(new AlterConfigOp(
                            new ConfigEntry(
                                    "consumer.assignment.interval.ms", "-1"),
                            AlterConfigOp.OpType.SET)));
            try {
                admin.incrementalAlterConfigs(invalid).all().get(10, TimeUnit.SECONDS);
                throw new AssertionError(
                        "out-of-bounds assignment interval was accepted");
            } catch (ExecutionException error) {
                if (!(error.getCause() instanceof InvalidRequestException)) {
                    throw error;
                }
            }

            admin.incrementalAlterConfigs(Map.of(
                            broker,
                            brokerKeys.stream()
                                    .map(key -> new AlterConfigOp(
                                            new ConfigEntry(key, null),
                                            AlterConfigOp.OpType.DELETE))
                                    .toList(),
                            group,
                            groupKeys.stream()
                                    .map(key -> new AlterConfigOp(
                                            new ConfigEntry(key, null),
                                            AlterConfigOp.OpType.DELETE))
                                    .toList()))
                    .all()
                    .get(10, TimeUnit.SECONDS);
        }
        System.out.println(
                "Kafka 4.3 KIP-1263 assignment interval defaults and group overrides passed");
    }

    private static void kip1196BufferControls(Properties properties) throws Exception {
        ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER, "");
        List<String> keys = List.of(
                "group.coordinator.cached.buffer.max.bytes",
                "share.coordinator.cached.buffer.max.bytes");
        try (Admin admin = Admin.create(properties)) {
            Map<String, ConfigEntry> defaults = describeConfigEntries(admin, broker);
            for (String key : keys) {
                ConfigEntry entry = defaults.get(key);
                if (entry == null
                        || !"1048588".equals(entry.value())
                        || entry.source()
                                != ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG
                        || entry.isReadOnly()
                        || entry.type() != ConfigEntry.ConfigType.INT
                        || entry.synonyms().size() != 1) {
                    throw new AssertionError(
                            "unexpected KIP-1196 default " + key + ": " + entry);
                }
            }

            admin.incrementalAlterConfigs(Map.of(
                            broker,
                            List.of(
                                    new AlterConfigOp(
                                            new ConfigEntry(keys.get(0), "524288"),
                                            AlterConfigOp.OpType.SET),
                                    new AlterConfigOp(
                                            new ConfigEntry(keys.get(1), "2097152"),
                                            AlterConfigOp.OpType.SET))))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            Map<String, ConfigEntry> dynamic = describeConfigEntries(admin, broker);
            for (int index = 0; index < keys.size(); index++) {
                ConfigEntry entry = dynamic.get(keys.get(index));
                String expected = index == 0 ? "524288" : "2097152";
                if (entry == null
                        || !expected.equals(entry.value())
                        || entry.source()
                                != ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG
                        || entry.synonyms().size() != 2) {
                    throw new AssertionError(
                            "unexpected dynamic KIP-1196 config "
                                    + keys.get(index)
                                    + ": "
                                    + entry);
                }
            }

            try {
                admin.incrementalAlterConfigs(Map.of(
                                broker,
                                List.of(new AlterConfigOp(
                                        new ConfigEntry(keys.get(0), "524287"),
                                        AlterConfigOp.OpType.SET))))
                        .all()
                        .get(10, TimeUnit.SECONDS);
                throw new AssertionError(
                        "out-of-bounds coordinator buffer limit was accepted");
            } catch (ExecutionException error) {
                if (!(error.getCause() instanceof InvalidRequestException)) {
                    throw error;
                }
            }
            if (!"524288".equals(
                    describeConfigEntries(admin, broker).get(keys.get(0)).value())) {
                throw new AssertionError(
                        "invalid coordinator buffer update partially mutated state");
            }

            admin.incrementalAlterConfigs(Map.of(
                            broker,
                            keys.stream()
                                    .map(key -> new AlterConfigOp(
                                            new ConfigEntry(key, null),
                                            AlterConfigOp.OpType.DELETE))
                                    .toList()))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            Map<String, ConfigEntry> restored = describeConfigEntries(admin, broker);
            for (String key : keys) {
                assertConfig(
                        restored,
                        key,
                        "1048588",
                        ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG);
            }
        }
        System.out.println(
                "Kafka 4.3 KIP-1196 coordinator buffer controls passed");
    }

    private static Map<String, ConfigEntry> describeConfigEntries(
            Admin admin, ConfigResource resource) throws Exception {
        return admin.describeConfigs(
                        List.of(resource),
                        new DescribeConfigsOptions().includeSynonyms(true))
                .all()
                .get(10, TimeUnit.SECONDS)
                .get(resource)
                .entries()
                .stream()
                .collect(java.util.stream.Collectors.toMap(
                        ConfigEntry::name, entry -> entry));
    }

    private static void assertConfig(
            Map<String, ConfigEntry> entries, String name, String expected) {
        assertConfig(
                entries,
                name,
                expected,
                ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG);
    }

    private static void assertConfig(
            Map<String, ConfigEntry> entries,
            String name,
            String expected,
            ConfigEntry.ConfigSource source) {
        ConfigEntry entry = entries.get(name);
        if (entry == null
                || !expected.equals(entry.value())
                || entry.source() != source) {
            throw new AssertionError(
                    "unexpected configuration " + name + ": " + entry);
        }
    }

    private static void provisionAcls(Properties common, String topic) throws Exception {
        String group = topic + "-group";
        String tokenOwner = "User:" + topic + "-owner";
        List<AclBinding> bindings = List.of(
                binding(ResourceType.TOPIC, topic, "User:alice", AclOperation.ALL),
                binding(ResourceType.GROUP, group, "User:alice", AclOperation.ALL),
                binding(ResourceType.TOPIC, topic, "User:dynamic", AclOperation.ALL),
                binding(ResourceType.GROUP, group, "User:dynamic", AclOperation.ALL),
                binding(
                        ResourceType.TRANSACTIONAL_ID,
                        topic + "-two-phase",
                        "User:alice",
                        AclOperation.TWO_PHASE_COMMIT),
                binding(
                        ResourceType.CLUSTER,
                        "kafka-cluster",
                        "User:alice",
                        AclOperation.IDEMPOTENT_WRITE),
                binding(
                        ResourceType.CLUSTER,
                        "kafka-cluster",
                        "User:dynamic",
                        AclOperation.IDEMPOTENT_WRITE),
                binding(
                        ResourceType.USER,
                        tokenOwner,
                        "User:alice",
                        AclOperation.CREATE_TOKENS),
                binding(
                        ResourceType.USER,
                        tokenOwner,
                        "User:dynamic",
                        AclOperation.DESCRIBE_TOKENS));
        AclBinding probe =
                binding(ResourceType.TOPIC, topic, "User:probe", AclOperation.DESCRIBE);
        try (Admin admin = Admin.create(common)) {
            admin.createAcls(bindings).all().get(10, TimeUnit.SECONDS);
            admin.createAcls(List.of(probe)).all().get(10, TimeUnit.SECONDS);
            var described = admin.describeAcls(AclBindingFilter.ANY)
                    .values()
                    .get(10, TimeUnit.SECONDS);
            if (!described.containsAll(bindings) || !described.contains(probe)) {
                throw new AssertionError("DescribeAcls did not return created ACLs");
            }
            admin.deleteAcls(List.of(probe.toFilter()))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            var afterDelete = admin.describeAcls(AclBindingFilter.ANY)
                    .values()
                    .get(10, TimeUnit.SECONDS);
            if (afterDelete.contains(probe)) {
                throw new AssertionError("DeleteAcls did not remove the probe ACL");
            }
            verifyDynamicScram(admin);
        }
    }

    private static void authorizationFiltering(Properties common, String prefix) throws Exception {
        String visibleTopic = prefix + "-visible-topic";
        String hiddenTopic = prefix + "-hidden-topic";
        String visibleTransaction = prefix + "-visible-tx";
        String hiddenTransaction = prefix + "-hidden-tx";
        String offsetTransaction = prefix + "-offset-tx";
        String addPartitionsTransaction = prefix + "-add-partitions-tx";
        String visibleGroup = prefix + "-visible-group";
        String hiddenGroup = prefix + "-hidden-group";
        createTopic(common, visibleTopic);
        createTopic(common, hiddenTopic);

        Properties visibleProducerProperties =
                transactionalProducerProperties(common, visibleTransaction);
        Properties hiddenProducerProperties =
                transactionalProducerProperties(common, hiddenTransaction);
        try (Admin admin = Admin.create(common);
                KafkaProducer<String, String> visibleProducer =
                        new KafkaProducer<>(visibleProducerProperties);
                KafkaProducer<String, String> hiddenProducer =
                        new KafkaProducer<>(hiddenProducerProperties)) {
            visibleProducer.initTransactions();
            visibleProducer.beginTransaction();
            visibleProducer.send(new ProducerRecord<>(
                            visibleTopic, 0, "visible", "visible"))
                    .get(10, TimeUnit.SECONDS);
            visibleProducer.send(new ProducerRecord<>(
                            hiddenTopic, 0, "hidden-participant", "hidden"))
                    .get(10, TimeUnit.SECONDS);

            hiddenProducer.initTransactions();
            hiddenProducer.beginTransaction();
            hiddenProducer.send(new ProducerRecord<>(
                            hiddenTopic, 0, "hidden", "hidden"))
                    .get(10, TimeUnit.SECONDS);

            TopicPartition visiblePartition = new TopicPartition(visibleTopic, 0);
            TopicPartition hiddenPartition = new TopicPartition(hiddenTopic, 0);
            admin.alterConsumerGroupOffsets(
                            visibleGroup,
                            Map.of(
                                    visiblePartition,
                                    new OffsetAndMetadata(0),
                                    hiddenPartition,
                                    new OffsetAndMetadata(0)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            admin.alterConsumerGroupOffsets(
                            hiddenGroup,
                            Map.of(visiblePartition, new OffsetAndMetadata(0)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            admin.createAcls(List.of(
                            binding(
                                    ResourceType.TRANSACTIONAL_ID,
                                    visibleTransaction,
                                    "User:alice",
                                    AclOperation.DESCRIBE),
                            binding(
                                    ResourceType.TRANSACTIONAL_ID,
                                    offsetTransaction,
                                    "User:alice",
                                    AclOperation.WRITE),
                            binding(
                                    ResourceType.TRANSACTIONAL_ID,
                                    addPartitionsTransaction,
                                    "User:alice",
                                    AclOperation.WRITE),
                            binding(
                                    ResourceType.TOPIC,
                                    visibleTopic,
                                    "User:alice",
                                    AclOperation.DESCRIBE),
                            binding(
                                    ResourceType.TOPIC,
                                    visibleTopic,
                                    "User:alice",
                                    AclOperation.DESCRIBE_CONFIGS),
                            binding(
                                    ResourceType.TOPIC,
                                    visibleTopic,
                                    "User:alice",
                                    AclOperation.ALTER_CONFIGS),
                            binding(
                                    ResourceType.TOPIC,
                                    visibleTopic,
                                    "User:alice",
                                    AclOperation.DELETE),
                            binding(
                                    ResourceType.TOPIC,
                                    visibleTopic,
                                    "User:alice",
                                    AclOperation.READ),
                            binding(
                                    ResourceType.TOPIC,
                                    visibleTopic,
                                    "User:alice",
                                    AclOperation.WRITE),
                            binding(
                                    ResourceType.TOPIC,
                                    visibleTopic,
                                    "User:bob",
                                    AclOperation.DESCRIBE),
                            binding(
                                    ResourceType.TOPIC,
                                    visibleTopic,
                                    "User:bob",
                                    AclOperation.WRITE),
                            binding(
                                    ResourceType.GROUP,
                                    visibleGroup,
                                    "User:alice",
                                    AclOperation.DESCRIBE),
                            binding(
                                    ResourceType.GROUP,
                                    visibleGroup,
                                    "User:alice",
                                    AclOperation.READ),
                            binding(
                                    ResourceType.GROUP,
                                    visibleGroup,
                                    "User:alice",
                                    AclOperation.DELETE)))
                    .all()
                    .get(10, TimeUnit.SECONDS);

            Properties alice = userProperties(common, "alice", "correct-horse");
            assertTopicOnlyIdempotentProducer(
                    userProperties(common, "bob", "bob-secret"), visibleTopic);
            assertLeastPrivilegeVisibility(
                    alice,
                    visibleTransaction,
                    hiddenTransaction,
                    visibleTopic,
                    hiddenTopic,
                    visibleGroup,
                    hiddenGroup,
                    addPartitionsTransaction,
                    false);

            admin.createAcls(List.of(binding(
                            ResourceType.CLUSTER,
                            "kafka-cluster",
                            "User:alice",
                            AclOperation.DESCRIBE)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            assertLeastPrivilegeVisibility(
                    alice,
                    visibleTransaction,
                    hiddenTransaction,
                    visibleTopic,
                    hiddenTopic,
                    visibleGroup,
                    hiddenGroup,
                    addPartitionsTransaction,
                    true);

            assertOffsetCommitAuthorization(
                    admin, alice, visibleTopic, hiddenTopic, visibleGroup);
            assertListOffsetsAuthorization(alice, visibleTopic, hiddenTopic);
            assertTxnOffsetCommitAuthorization(
                    admin,
                    alice,
                    offsetTransaction,
                    visibleTopic,
                    hiddenTopic,
                    visibleGroup);
            visibleProducer.abortTransaction();
            hiddenProducer.abortTransaction();
        }
        System.out.println(
                "Kafka 4.3 DescribeCluster, DescribeConfigs, AlterConfigs, Produce, Fetch, ListOffsets, OffsetCommit, AddPartitionsToTxn, TxnOffsetCommit, transaction, and group ACL filtering passed");
    }

    private static void assertTopicOnlyIdempotentProducer(
            Properties properties, String topic) throws Exception {
        Properties producerProperties = new Properties();
        producerProperties.putAll(properties);
        producerProperties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        producerProperties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProperties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000");
        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(producerProperties)) {
            producer.send(new ProducerRecord<>(
                            topic, 0, "topic-only-idempotent", "accepted"))
                    .get(10, TimeUnit.SECONDS);
        }
    }

    private static void assertListOffsetsAuthorization(
            Properties properties, String visibleTopic, String hiddenTopic)
            throws Exception {
        TopicPartition visible = new TopicPartition(visibleTopic, 0);
        TopicPartition hidden = new TopicPartition(hiddenTopic, 0);
        Map<TopicPartition, OffsetSpec> offsets = new LinkedHashMap<>();
        offsets.put(hidden, OffsetSpec.latest());
        offsets.put(visible, OffsetSpec.latest());
        try (Admin restricted = Admin.create(properties)) {
            var result = restricted.listOffsets(offsets);
            long visibleOffset = result
                    .partitionResult(visible)
                    .get(10, TimeUnit.SECONDS)
                    .offset();
            if (visibleOffset < 0) {
                throw new AssertionError(
                        "ListOffsets did not return the authorized partition");
            }
            try {
                result
                        .partitionResult(hidden)
                        .get(10, TimeUnit.SECONDS);
                throw new AssertionError(
                        "ListOffsets returned an unauthorized partition");
            } catch (ExecutionException error) {
                if (!(error.getCause() instanceof TopicAuthorizationException topicError)
                        || !topicError.unauthorizedTopics().contains(hiddenTopic)) {
                    throw error;
                }
            }
        }
    }

    private static void assertOffsetCommitAuthorization(
            Admin admin,
            Properties properties,
            String visibleTopic,
            String hiddenTopic,
            String group)
            throws Exception {
        TopicPartition visible = new TopicPartition(visibleTopic, 0);
        TopicPartition hidden = new TopicPartition(hiddenTopic, 0);
        try (Admin restricted = Admin.create(properties)) {
            try {
                restricted.alterConsumerGroupOffsets(
                                group,
                                Map.of(
                                        hidden, new OffsetAndMetadata(2),
                                        visible, new OffsetAndMetadata(3)))
                        .all()
                        .get(10, TimeUnit.SECONDS);
                throw new AssertionError(
                        "OffsetCommit accepted an unauthorized topic");
            } catch (ExecutionException error) {
                if (!(error.getCause() instanceof TopicAuthorizationException topicError)
                        || topicError.getMessage() == null
                        || !topicError.getMessage().contains(hiddenTopic)) {
                    throw error;
                }
            }
        }
        Map<TopicPartition, OffsetAndMetadata> committed = admin
                .listConsumerGroupOffsets(group)
                .partitionsToOffsetAndMetadata()
                .get(10, TimeUnit.SECONDS);
        if (committed.get(visible) == null
                || committed.get(visible).offset() != 3
                || committed.get(hidden) == null
                || committed.get(hidden).offset() != 0) {
            throw new AssertionError(
                    "OffsetCommit mixed authorization semantics diverged: "
                            + committed);
        }
        try (Admin restricted = Admin.create(properties)) {
            Map<TopicPartition, OffsetAndMetadata> visibleOffsets = restricted
                    .listConsumerGroupOffsets(group)
                    .partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS);
            if (visibleOffsets.get(visible) == null
                    || visibleOffsets.get(visible).offset() != 3
                    || visibleOffsets.containsKey(hidden)) {
                throw new AssertionError(
                        "OffsetFetch all-topic authorization leaked offsets: "
                                + visibleOffsets);
            }
        }
    }

    private static void assertTxnOffsetCommitAuthorization(
            Admin admin,
            Properties properties,
            String transactionalId,
            String visibleTopic,
            String hiddenTopic,
            String group)
            throws Exception {
        TopicPartition visible = new TopicPartition(visibleTopic, 0);
        TopicPartition hidden = new TopicPartition(hiddenTopic, 0);
        Properties producerProperties =
                transactionalProducerProperties(properties, transactionalId);
        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(producerProperties)) {
            producer.initTransactions();
            producer.beginTransaction();
            try {
                producer.sendOffsetsToTransaction(
                        Map.of(
                                hidden, new OffsetAndMetadata(5),
                                visible, new OffsetAndMetadata(6)),
                        new ConsumerGroupMetadata(group));
                throw new AssertionError(
                        "TxnOffsetCommit accepted an unauthorized topic");
            } catch (KafkaException error) {
                boolean expected = error instanceof TopicAuthorizationException topicError
                        ? topicError.unauthorizedTopics().contains(hiddenTopic)
                        : error.getMessage() != null
                                && error.getMessage().contains(
                                        "Topic authorization failed");
                if (!expected) {
                    throw error;
                }
            }
        }
        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(producerProperties)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.sendOffsetsToTransaction(
                    Map.of(visible, new OffsetAndMetadata(7)),
                    new ConsumerGroupMetadata(group));
            producer.commitTransaction();
        }

        Map<TopicPartition, OffsetAndMetadata> committed = admin
                .listConsumerGroupOffsets(group)
                .partitionsToOffsetAndMetadata()
                .get(10, TimeUnit.SECONDS);
        if (committed.get(visible) == null
                || committed.get(visible).offset() != 7
                || committed.get(hidden) == null
                || committed.get(hidden).offset() != 0) {
            throw new AssertionError(
                    "TxnOffsetCommit authorization or abort semantics diverged: "
                            + committed);
        }
    }

    private static void assertLeastPrivilegeVisibility(
            Properties properties,
            String visibleTransaction,
            String hiddenTransaction,
            String visibleTopic,
            String hiddenTopic,
            String visibleGroup,
            String hiddenGroup,
            String addPartitionsTransaction,
            boolean clusterDescribe)
            throws Exception {
        try (Admin admin = Admin.create(properties)) {
            var cluster = admin.describeCluster(
                    new DescribeClusterOptions().includeAuthorizedOperations(true));
            if (cluster.clusterId().get(10, TimeUnit.SECONDS).isBlank()
                    || cluster.nodes().get(10, TimeUnit.SECONDS).isEmpty()) {
                throw new AssertionError("DescribeCluster did not return broker topology");
            }
            Set<AclOperation> clusterOperations =
                    cluster.authorizedOperations().get(10, TimeUnit.SECONDS);
            if (clusterDescribe
                    ? !clusterOperations.contains(AclOperation.DESCRIBE)
                    : !clusterOperations.isEmpty()) {
                throw new AssertionError(
                        "DescribeCluster authorization filtering failed: "
                                + clusterOperations);
            }

            ConfigResource visibleConfig =
                    new ConfigResource(ConfigResource.Type.TOPIC, visibleTopic);
            ConfigResource hiddenConfig =
                    new ConfigResource(ConfigResource.Type.TOPIC, hiddenTopic);
            var configResults =
                    admin.describeConfigs(List.of(hiddenConfig, visibleConfig)).values();
            if (configResults.get(visibleConfig)
                    .get(10, TimeUnit.SECONDS)
                    .entries()
                    .isEmpty()) {
                throw new AssertionError("visible topic configuration was empty");
            }
            try {
                configResults.get(hiddenConfig).get(10, TimeUnit.SECONDS);
                throw new AssertionError("hidden topic configuration was disclosed");
            } catch (ExecutionException error) {
                if (!(error.getCause() instanceof TopicAuthorizationException)) {
                    throw error;
                }
            }

            Collection<AlterConfigOp> retentionChange = List.of(new AlterConfigOp(
                    new ConfigEntry("retention.ms", "60000"),
                    AlterConfigOp.OpType.SET));
            Map<ConfigResource, Collection<AlterConfigOp>> changes =
                    Map.of(hiddenConfig, retentionChange, visibleConfig, retentionChange);
            var alterResults = admin.incrementalAlterConfigs(changes).values();
            alterResults.get(visibleConfig).get(10, TimeUnit.SECONDS);
            try {
                alterResults.get(hiddenConfig).get(10, TimeUnit.SECONDS);
                throw new AssertionError("hidden topic configuration was changed");
            } catch (ExecutionException error) {
                if (!(error.getCause() instanceof TopicAuthorizationException)) {
                    throw error;
                }
            }
            String retention = admin.describeConfigs(List.of(visibleConfig))
                    .all()
                    .get(10, TimeUnit.SECONDS)
                    .get(visibleConfig)
                    .get("retention.ms")
                    .value();
            if (!"60000".equals(retention)) {
                throw new AssertionError(
                        "authorized topic configuration was not changed: " + retention);
            }
            if (!clusterDescribe) {
                assertPartitionAuthorization(
                        properties,
                        visibleTopic,
                        hiddenTopic,
                        visibleGroup,
                        addPartitionsTransaction);
            }

            Set<String> transactions = admin.listTransactions()
                    .all()
                    .get(10, TimeUnit.SECONDS)
                    .stream()
                    .map(transaction -> transaction.transactionalId())
                    .collect(java.util.stream.Collectors.toSet());
            if (!transactions.contains(visibleTransaction)
                    || transactions.contains(hiddenTransaction)) {
                throw new AssertionError(
                        "transactional id filtering failed: " + transactions);
            }

            var description = admin.describeTransactions(List.of(visibleTransaction))
                    .all()
                    .get(10, TimeUnit.SECONDS)
                    .get(visibleTransaction);
            Set<TopicPartition> partitions = description.topicPartitions();
            if (!partitions.contains(new TopicPartition(visibleTopic, 0))
                    || partitions.contains(new TopicPartition(hiddenTopic, 0))) {
                throw new AssertionError(
                        "transaction topic filtering failed: " + partitions);
            }

            Set<String> groups = admin.listGroups()
                    .all()
                    .get(10, TimeUnit.SECONDS)
                    .stream()
                    .map(group -> group.groupId())
                    .collect(java.util.stream.Collectors.toSet());
            if (!groups.contains(visibleGroup)
                    || groups.contains(hiddenGroup) != clusterDescribe) {
                throw new AssertionError("group filtering failed: " + groups);
            }
        }
    }

    private static void assertPartitionAuthorization(
            Properties properties,
            String visibleTopic,
            String hiddenTopic,
            String visibleGroup,
            String addPartitionsTransaction)
            throws Exception {
        AdminClientConfig config = new AdminClientConfig(properties);
        Time time = Time.SYSTEM;
        Metrics metrics = new Metrics();
        String bootstrap = properties
                .getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)
                .split(",", 2)[0];
        int separator = bootstrap.lastIndexOf(':');
        String host = bootstrap.substring(0, separator);
        int port = Integer.parseInt(bootstrap.substring(separator + 1));
        Node node = new Node(0, host, port);
        LogContext logContext = new LogContext("[epoch-security] ");
        Metadata metadata = new Metadata(
                config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG),
                config.getLong(AdminClientConfig.RETRY_BACKOFF_MAX_MS_CONFIG),
                config.getLong(AdminClientConfig.METADATA_MAX_AGE_CONFIG),
                logContext,
                new ClusterResourceListeners());
        metadata.bootstrap(List.of(new InetSocketAddress(host, port)));
        NetworkClient client = ClientUtils.createNetworkClient(
                config,
                metrics,
                "epoch-security",
                logContext,
                new ApiVersions(),
                time,
                1,
                metadata,
                null,
                null);
        try {
            if (!NetworkClientUtils.awaitReady(client, node, time, 10_000)) {
                throw new AssertionError(
                        "partition authorization security client was not ready");
            }
            OffsetForLeaderTopicCollection topics =
                    new OffsetForLeaderTopicCollection();
            topics.add(epochTopic(hiddenTopic));
            topics.add(epochTopic(visibleTopic));
            ClientRequest request = client.newClientRequest(
                    node.idString(),
                    OffsetsForLeaderEpochRequest.Builder.forConsumer(topics),
                    time.milliseconds(),
                    true);
            ClientResponse response =
                    NetworkClientUtils.sendAndReceive(client, request, time);
            OffsetsForLeaderEpochResponse epochs =
                    (OffsetsForLeaderEpochResponse) response.responseBody();
            var results = epochs.data().topics();
            if (results.size() != 2) {
                throw new AssertionError(
                        "unexpected OffsetForLeaderEpoch response: "
                                + epochs.data());
            }
            var iterator = results.iterator();
            var visible = iterator.next();
            var hidden = iterator.next();
            if (!visibleTopic.equals(visible.topic())
                    || visible.partitions().get(0).errorCode() != Errors.NONE.code()
                    || !hiddenTopic.equals(hidden.topic())
                    || hidden.partitions().get(0).errorCode()
                            != Errors.TOPIC_AUTHORIZATION_FAILED.code()) {
                throw new AssertionError(
                        "OffsetForLeaderEpoch authorization assembly failed: "
                                + epochs.data());
            }
            assertRawListOffsetsAuthorization(
                    client, node, time, visibleTopic, hiddenTopic);
            assertFetchAuthorization(
                    client, node, time, visibleTopic, hiddenTopic);
            assertProduceAuthorization(
                    client, node, time, visibleTopic, hiddenTopic);
            assertAddPartitionsToTxnAuthorization(
                    client,
                    node,
                    time,
                    addPartitionsTransaction,
                    visibleTopic,
                    hiddenTopic);
            assertDeleteRecordsAuthorization(
                    client, node, time, visibleTopic, hiddenTopic);
            assertOffsetDeleteAuthorization(
                    client,
                    node,
                    time,
                    visibleTopic,
                    hiddenTopic,
                    visibleGroup);
            assertDelegationTokenOwnerDefaulting(client, node, time);
        } finally {
            client.close();
            metrics.close();
        }
    }

    private static void assertDelegationTokenOwnerDefaulting(
            NetworkClient client, Node node, Time time) throws Exception {
        for (String ownerName : new String[] {null, ""}) {
            CreateDelegationTokenRequestData createData =
                    new CreateDelegationTokenRequestData()
                            .setOwnerPrincipalType("Service")
                            .setOwnerPrincipalName(ownerName)
                            .setMaxLifetimeMs(60_000);
            ClientRequest createRequest = client.newClientRequest(
                    node.idString(),
                    new CreateDelegationTokenRequest.Builder(createData),
                    time.milliseconds(),
                    true);
            CreateDelegationTokenResponse createResponse =
                    (CreateDelegationTokenResponse)
                            NetworkClientUtils.sendAndReceive(
                                            client, createRequest, time)
                                    .responseBody();
            if (createResponse.error() != Errors.NONE
                    || !"User".equals(createResponse.data().principalType())
                    || !"alice".equals(createResponse.data().principalName())
                    || !"User".equals(
                            createResponse.data().tokenRequesterPrincipalType())
                    || !"alice".equals(
                            createResponse.data().tokenRequesterPrincipalName())) {
                throw new AssertionError(
                        "CreateDelegationToken owner defaulting diverged: "
                                + createResponse.data());
            }

            ExpireDelegationTokenRequestData expireData =
                    new ExpireDelegationTokenRequestData()
                            .setHmac(createResponse.data().hmac())
                            .setExpiryTimePeriodMs(-1);
            ClientRequest expireRequest = client.newClientRequest(
                    node.idString(),
                    new ExpireDelegationTokenRequest.Builder(expireData),
                    time.milliseconds(),
                    true);
            ExpireDelegationTokenResponse expireResponse =
                    (ExpireDelegationTokenResponse)
                            NetworkClientUtils.sendAndReceive(
                                            client, expireRequest, time)
                                    .responseBody();
            if (expireResponse.error() != Errors.NONE) {
                throw new AssertionError(
                        "requester could not expire default-owned token: "
                                + expireResponse.data());
            }
        }
    }

    private static void assertAddPartitionsToTxnAuthorization(
            NetworkClient client,
            Node node,
            Time time,
            String transactionalId,
            String visibleTopic,
            String hiddenTopic)
            throws Exception {
        InitProducerIdRequestData initData = new InitProducerIdRequestData()
                .setTransactionalId(transactionalId)
                .setTransactionTimeoutMs(60_000);
        ClientRequest initRequest = client.newClientRequest(
                node.idString(),
                new InitProducerIdRequest.Builder(initData),
                time.milliseconds(),
                true);
        InitProducerIdResponse initResponse = (InitProducerIdResponse)
                NetworkClientUtils.sendAndReceive(client, initRequest, time)
                        .responseBody();
        if (initResponse.error() != Errors.NONE) {
            throw new AssertionError(
                    "InitProducerId for AddPartitionsToTxn failed: "
                            + initResponse.data());
        }

        TopicPartition visible = new TopicPartition(visibleTopic, 0);
        TopicPartition hidden = new TopicPartition(hiddenTopic, 0);
        AddPartitionsToTxnResponse mixedResponse = sendAddPartitionsToTxn(
                client,
                node,
                time,
                transactionalId,
                initResponse.data().producerId(),
                initResponse.data().producerEpoch(),
                List.of(hidden, visible));
        Map<TopicPartition, Errors> mixed = mixedResponse
                .errors()
                .get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID);
        if (mixed == null
                || mixed.get(hidden) != Errors.TOPIC_AUTHORIZATION_FAILED
                || mixed.get(visible) != Errors.OPERATION_NOT_ATTEMPTED) {
            throw new AssertionError(
                    "AddPartitionsToTxn mixed authorization was not atomic: "
                            + mixedResponse.data());
        }

        AddPartitionsToTxnResponse visibleResponse = sendAddPartitionsToTxn(
                client,
                node,
                time,
                transactionalId,
                initResponse.data().producerId(),
                initResponse.data().producerEpoch(),
                List.of(visible));
        Map<TopicPartition, Errors> visibleOnly = visibleResponse
                .errors()
                .get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID);
        if (visibleOnly == null || visibleOnly.get(visible) != Errors.NONE) {
            throw new AssertionError(
                    "AddPartitionsToTxn authorized retry failed: "
                            + visibleResponse.data());
        }
    }

    private static AddPartitionsToTxnResponse sendAddPartitionsToTxn(
            NetworkClient client,
            Node node,
            Time time,
            String transactionalId,
            long producerId,
            short producerEpoch,
            List<TopicPartition> partitions)
            throws Exception {
        ClientRequest request = client.newClientRequest(
                node.idString(),
                AddPartitionsToTxnRequest.Builder.forClient(
                        transactionalId, producerId, producerEpoch, partitions),
                time.milliseconds(),
                true);
        return (AddPartitionsToTxnResponse)
                NetworkClientUtils.sendAndReceive(client, request, time)
                        .responseBody();
    }

    private static void assertProduceAuthorization(
            NetworkClient client,
            Node node,
            Time time,
            String visibleTopic,
            String hiddenTopic)
            throws Exception {
        ProduceRequestData data = new ProduceRequestData()
                .setAcks((short) 1)
                .setTimeoutMs(5_000)
                .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
                        List.of(
                                        produceTopic(hiddenTopic, "hidden"),
                                        produceTopic(visibleTopic, "visible"))
                                .iterator()));
        ClientRequest request = client.newClientRequest(
                node.idString(),
                new ProduceRequest.Builder((short) 12, (short) 12, data),
                time.milliseconds(),
                true);
        ProduceResponse response = (ProduceResponse)
                NetworkClientUtils.sendAndReceive(client, request, time)
                        .responseBody();
        if (response.data().responses().size() != 2) {
            throw new AssertionError(
                    "unexpected Produce response: " + response.data());
        }
        var iterator = response.data().responses().iterator();
        var visible = iterator.next();
        var hidden = iterator.next();
        if (!visibleTopic.equals(visible.name())
                || visible.partitionResponses().get(0).errorCode()
                        != Errors.NONE.code()
                || visible.partitionResponses().get(0).baseOffset() < 0
                || !hiddenTopic.equals(hidden.name())
                || hidden.partitionResponses().get(0).errorCode()
                        != Errors.TOPIC_AUTHORIZATION_FAILED.code()
                || hidden.partitionResponses().get(0).baseOffset() != -1) {
            throw new AssertionError(
                    "Produce authorization assembly failed: "
                            + response.data());
        }
    }

    private static void assertFetchAuthorization(
            NetworkClient client,
            Node node,
            Time time,
            String visibleTopic,
            String hiddenTopic)
            throws Exception {
        Map<TopicPartition, FetchRequest.PartitionData> fetchData =
                new LinkedHashMap<>();
        fetchData.put(
                new TopicPartition(hiddenTopic, 0),
                new FetchRequest.PartitionData(
                        Uuid.ZERO_UUID,
                        0,
                        FetchRequest.INVALID_LOG_START_OFFSET,
                        1024 * 1024,
                        Optional.of(0)));
        fetchData.put(
                new TopicPartition(visibleTopic, 0),
                new FetchRequest.PartitionData(
                        Uuid.ZERO_UUID,
                        0,
                        FetchRequest.INVALID_LOG_START_OFFSET,
                        1024 * 1024,
                        Optional.of(0)));
        ClientRequest request = client.newClientRequest(
                node.idString(),
                FetchRequest.Builder.forConsumer(
                                (short) 12, 0, 1, fetchData)
                        .setMaxBytes(2 * 1024 * 1024),
                time.milliseconds(),
                true);
        FetchResponse response = (FetchResponse)
                NetworkClientUtils.sendAndReceive(client, request, time)
                        .responseBody();
        if (response.data().errorCode() != Errors.NONE.code()
                || response.data().responses().size() != 2) {
            throw new AssertionError(
                    "unexpected Fetch response: " + response.data());
        }
        var visible = response.data().responses().get(0);
        var hidden = response.data().responses().get(1);
        var visiblePartition = visible.partitions().get(0);
        var hiddenPartition = hidden.partitions().get(0);
        if (!visibleTopic.equals(visible.topic())
                || visiblePartition.errorCode() != Errors.NONE.code()
                || visiblePartition.records() == null
                || visiblePartition.records().sizeInBytes() == 0
                || !hiddenTopic.equals(hidden.topic())
                || hiddenPartition.errorCode()
                        != Errors.TOPIC_AUTHORIZATION_FAILED.code()
                || hiddenPartition.highWatermark() != -1
                || hiddenPartition.records() == null
                || hiddenPartition.records().sizeInBytes() != 0) {
            throw new AssertionError(
                    "Fetch authorization assembly failed: "
                            + response.data());
        }
    }

    private static ProduceRequestData.TopicProduceData produceTopic(
            String topic, String value) {
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(value.getBytes(StandardCharsets.UTF_8)));
        return new ProduceRequestData.TopicProduceData()
                .setName(topic)
                .setPartitionData(List.of(
                        new ProduceRequestData.PartitionProduceData()
                                .setIndex(0)
                                .setRecords(records)));
    }

    private static void assertRawListOffsetsAuthorization(
            NetworkClient client,
            Node node,
            Time time,
            String visibleTopic,
            String hiddenTopic)
            throws Exception {
        List<ListOffsetsTopic> topics =
                List.of(listOffsetsTopic(hiddenTopic), listOffsetsTopic(visibleTopic));
        ListOffsetsRequest.Builder builder = ListOffsetsRequest.Builder
                .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
                .setTargetTimes(topics)
                .setTimeoutMs(5_000);
        ClientRequest request = client.newClientRequest(
                node.idString(), builder, time.milliseconds(), true);
        ListOffsetsResponse response = (ListOffsetsResponse)
                NetworkClientUtils.sendAndReceive(client, request, time)
                        .responseBody();
        if (response.data().topics().size() != 2) {
            throw new AssertionError(
                    "unexpected ListOffsets response: " + response.data());
        }
        var visible = response.data().topics().get(0);
        var hidden = response.data().topics().get(1);
        if (!visibleTopic.equals(visible.name())
                || visible.partitions().get(0).errorCode() != Errors.NONE.code()
                || !hiddenTopic.equals(hidden.name())
                || hidden.partitions().get(0).errorCode()
                        != Errors.TOPIC_AUTHORIZATION_FAILED.code()) {
            throw new AssertionError(
                    "ListOffsets authorization assembly failed: "
                            + response.data());
        }
    }

    private static ListOffsetsTopic listOffsetsTopic(String topic) {
        ListOffsetsTopic result = new ListOffsetsTopic().setName(topic);
        result.partitions().add(new ListOffsetsPartition()
                .setPartitionIndex(0)
                .setCurrentLeaderEpoch(0)
                .setTimestamp(-1));
        return result;
    }

    private static void assertDeleteRecordsAuthorization(
            NetworkClient client,
            Node node,
            Time time,
            String visibleTopic,
            String hiddenTopic)
            throws Exception {
        DeleteRecordsRequestData data =
                new DeleteRecordsRequestData().setTimeoutMs(5_000);
        data.topics().add(deleteRecordsTopic(hiddenTopic));
        data.topics().add(deleteRecordsTopic(visibleTopic));
        ClientRequest request = client.newClientRequest(
                node.idString(),
                new DeleteRecordsRequest.Builder(data),
                time.milliseconds(),
                true);
        DeleteRecordsResponse response = (DeleteRecordsResponse)
                NetworkClientUtils.sendAndReceive(client, request, time)
                        .responseBody();
        var hidden = response.data().topics().stream()
                .filter(topic -> hiddenTopic.equals(topic.name()))
                .findFirst()
                .orElseThrow();
        var visible = response.data().topics().stream()
                .filter(topic -> visibleTopic.equals(topic.name()))
                .findFirst()
                .orElseThrow();
        var hiddenPartition = hidden.partitions().find(0);
        var visiblePartition = visible.partitions().find(0);
        if (hiddenPartition == null
                || hiddenPartition.errorCode()
                        != Errors.TOPIC_AUTHORIZATION_FAILED.code()
                || visiblePartition == null
                || visiblePartition.errorCode() != Errors.NONE.code()
                || visiblePartition.lowWatermark() != 0) {
            throw new AssertionError(
                    "DeleteRecords authorization failed: " + response.data());
        }
    }

    private static DeleteRecordsTopic deleteRecordsTopic(String topic) {
        DeleteRecordsTopic result = new DeleteRecordsTopic().setName(topic);
        result.partitions().add(new DeleteRecordsPartition()
                .setPartitionIndex(0)
                .setOffset(0));
        return result;
    }

    private static void assertOffsetDeleteAuthorization(
            NetworkClient client,
            Node node,
            Time time,
            String visibleTopic,
            String hiddenTopic,
            String visibleGroup)
            throws Exception {
        OffsetDeleteRequestData data =
                new OffsetDeleteRequestData().setGroupId(visibleGroup);
        data.topics().add(offsetDeleteTopic(hiddenTopic));
        data.topics().add(offsetDeleteTopic(visibleTopic));
        ClientRequest request = client.newClientRequest(
                node.idString(),
                new OffsetDeleteRequest.Builder(data),
                time.milliseconds(),
                true);
        OffsetDeleteResponse response = (OffsetDeleteResponse)
                NetworkClientUtils.sendAndReceive(client, request, time)
                        .responseBody();
        var hidden = response.data().topics().find(hiddenTopic);
        var visible = response.data().topics().find(visibleTopic);
        var hiddenPartition =
                hidden == null ? null : hidden.partitions().find(0);
        var visiblePartition =
                visible == null ? null : visible.partitions().find(0);
        if (response.data().errorCode() != Errors.NONE.code()
                || hiddenPartition == null
                || hiddenPartition.errorCode()
                        != Errors.TOPIC_AUTHORIZATION_FAILED.code()
                || visiblePartition == null
                || visiblePartition.errorCode() != Errors.NONE.code()) {
            throw new AssertionError(
                    "OffsetDelete authorization failed: " + response.data());
        }
    }

    private static OffsetDeleteRequestTopic offsetDeleteTopic(String topic) {
        OffsetDeleteRequestTopic result =
                new OffsetDeleteRequestTopic().setName(topic);
        result.partitions().add(
                new OffsetDeleteRequestPartition().setPartitionIndex(0));
        return result;
    }

    private static OffsetForLeaderTopic epochTopic(String topic) {
        OffsetForLeaderTopic result = new OffsetForLeaderTopic().setTopic(topic);
        result.partitions().add(new OffsetForLeaderPartition()
                .setPartition(0)
                .setCurrentLeaderEpoch(0)
                .setLeaderEpoch(0));
        return result;
    }

    private static void verifyDynamicScram(Admin admin) throws Exception {
        for (ScramMechanism mechanism :
                List.of(ScramMechanism.SCRAM_SHA_256, ScramMechanism.SCRAM_SHA_512)) {
            admin.alterUserScramCredentials(List.of(
                            new UserScramCredentialUpsertion(
                                    "dynamic",
                                    new ScramCredentialInfo(mechanism, 4096),
                                    "dynamic-secret")))
                    .all()
                    .get(10, TimeUnit.SECONDS);
        }
        var description = admin.describeUserScramCredentials(List.of("dynamic"))
                .description("dynamic")
                .get(10, TimeUnit.SECONDS);
        if (description.credentialInfos().size() != 2) {
            throw new AssertionError(
                    "dynamic SCRAM mechanisms were not described: " + description);
        }

        admin.alterUserScramCredentials(List.of(
                        new UserScramCredentialUpsertion(
                                "dynamic-delete-probe",
                                new ScramCredentialInfo(
                                        ScramMechanism.SCRAM_SHA_256, 4096),
                                "delete-secret")))
                .all()
                .get(10, TimeUnit.SECONDS);
        admin.alterUserScramCredentials(List.of(
                        new UserScramCredentialDeletion(
                                "dynamic-delete-probe",
                                ScramMechanism.SCRAM_SHA_256)))
                .all()
                .get(10, TimeUnit.SECONDS);
    }

    private static void delegationTokenSmoke(
            Properties common, String mechanism, String topic) throws Exception {
        DelegationToken token;
        KafkaPrincipal owner = new KafkaPrincipal("User", "alice");
        KafkaPrincipal renewer = new KafkaPrincipal("User", "admin");
        List<KafkaPrincipal> renewers = List.of(
                new KafkaPrincipal("User", ""), renewer, renewer);
        try (Admin admin = Admin.create(common)) {
            token = admin.createDelegationToken(
                            new CreateDelegationTokenOptions()
                                    .owner(owner)
                                    .renewers(renewers)
                                    .maxLifetimeMs(120_000))
                    .delegationToken()
                    .get(10, TimeUnit.SECONDS);
            if (!token.tokenInfo().owner().equals(owner)
                    || !token.tokenInfo().tokenRequester().equals(renewer)
                    || token.hmac().length != 64) {
                throw new AssertionError("invalid delegation token response " + token);
            }
            long renewed = admin.renewDelegationToken(
                            token.hmac(),
                            new RenewDelegationTokenOptions().renewTimePeriodMs(60_000))
                    .expiryTimestamp()
                    .get(10, TimeUnit.SECONDS);
            if (renewed > token.tokenInfo().maxTimestamp()) {
                throw new AssertionError("delegation token renewed beyond max lifetime");
            }
            DelegationToken described = admin.describeDelegationToken()
                    .delegationTokens()
                    .get(10, TimeUnit.SECONDS)
                    .stream()
                    .filter(candidate -> candidate.tokenInfo()
                            .tokenId()
                            .equals(token.tokenInfo().tokenId()))
                    .findFirst()
                    .orElse(null);
            if (described == null) {
                throw new AssertionError("created delegation token was not described");
            }
            if (!described.tokenInfo().renewers().equals(renewers)) {
                throw new AssertionError(
                        "delegation token renewers were not preserved: "
                                + described.tokenInfo().renewers());
            }
        }

        Properties tokenProperties = tokenProperties(common, mechanism, token);
        produce(tokenProperties, topic);
        consume(tokenProperties, topic);
        assertTokenManagementErrorSemantics(tokenProperties, token);
        try (Admin tokenAdmin = Admin.create(tokenProperties)) {
            try {
                tokenAdmin.describeDelegationToken()
                        .delegationTokens()
                        .get(10, TimeUnit.SECONDS);
                throw new AssertionError(
                        "token-authenticated connection managed delegation tokens");
            } catch (ExecutionException error) {
                if (!(error.getCause() instanceof UnsupportedByAuthenticationException)) {
                    throw error;
                }
            }
        }

        try (Admin admin = Admin.create(common)) {
            admin.expireDelegationToken(
                            token.hmac(),
                            new ExpireDelegationTokenOptions().expiryTimePeriodMs(-1))
                    .expiryTimestamp()
                    .get(10, TimeUnit.SECONDS);
        }
        try (Admin expiredTokenAdmin = Admin.create(tokenProperties)) {
            try {
                expiredTokenAdmin.listTopics().names().get(10, TimeUnit.SECONDS);
                throw new AssertionError("expired delegation token authenticated");
            } catch (ExecutionException error) {
                if (!(error.getCause() instanceof SaslAuthenticationException)) {
                    throw error;
                }
            }
        }
        System.out.printf("%s delegation token SCRAM smoke passed%n", mechanism);
    }

    private static void assertTokenManagementErrorSemantics(
            Properties properties, DelegationToken token) throws Exception {
        AdminClientConfig config = new AdminClientConfig(properties);
        Time time = Time.SYSTEM;
        Metrics metrics = new Metrics();
        String bootstrap = properties
                .getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)
                .split(",", 2)[0];
        int separator = bootstrap.lastIndexOf(':');
        String host = bootstrap.substring(0, separator);
        int port = Integer.parseInt(bootstrap.substring(separator + 1));
        Node node = new Node(0, host, port);
        LogContext logContext = new LogContext("[delegation-token-errors] ");
        Metadata metadata = new Metadata(
                config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG),
                config.getLong(AdminClientConfig.RETRY_BACKOFF_MAX_MS_CONFIG),
                config.getLong(AdminClientConfig.METADATA_MAX_AGE_CONFIG),
                logContext,
                new ClusterResourceListeners());
        metadata.bootstrap(List.of(new InetSocketAddress(host, port)));
        NetworkClient client = ClientUtils.createNetworkClient(
                config,
                metrics,
                "delegation-token-errors",
                logContext,
                new ApiVersions(),
                time,
                1,
                metadata,
                null,
                null);
        try {
            if (!NetworkClientUtils.awaitReady(client, node, time, 10_000)) {
                throw new AssertionError(
                        "delegation-token error client was not ready");
            }

            CreateDelegationTokenRequestData createData =
                    new CreateDelegationTokenRequestData()
                            .setOwnerPrincipalType("User")
                            .setOwnerPrincipalName("alice")
                            .setMaxLifetimeMs(60_000);
            ClientRequest createRequest = client.newClientRequest(
                    node.idString(),
                    new CreateDelegationTokenRequest.Builder(createData),
                    time.milliseconds(),
                    true);
            CreateDelegationTokenResponse createResponse =
                    (CreateDelegationTokenResponse)
                            NetworkClientUtils.sendAndReceive(
                                            client, createRequest, time)
                                    .responseBody();
            if (createResponse.error()
                            != Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                    || !"User".equals(createResponse.data().principalType())
                    || !"alice".equals(createResponse.data().principalName())
                    || !"User".equals(
                            createResponse.data().tokenRequesterPrincipalType())
                    || !"alice".equals(
                            createResponse.data().tokenRequesterPrincipalName())
                    || createResponse.data().issueTimestampMs() != -1
                    || createResponse.data().expiryTimestampMs() != -1
                    || createResponse.data().maxTimestampMs() != -1) {
                throw new AssertionError(
                        "CreateDelegationToken error fields diverged: "
                                + createResponse.data());
            }

            RenewDelegationTokenRequestData renewData =
                    new RenewDelegationTokenRequestData()
                            .setHmac(token.hmac())
                            .setRenewPeriodMs(60_000);
            ClientRequest renewRequest = client.newClientRequest(
                    node.idString(),
                    new RenewDelegationTokenRequest.Builder(renewData),
                    time.milliseconds(),
                    true);
            RenewDelegationTokenResponse renewResponse =
                    (RenewDelegationTokenResponse)
                            NetworkClientUtils.sendAndReceive(
                                            client, renewRequest, time)
                                    .responseBody();
            if (renewResponse.error()
                            != Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                    || renewResponse.expiryTimestamp() != -1) {
                throw new AssertionError(
                        "RenewDelegationToken error fields diverged: "
                                + renewResponse.data());
            }

            ExpireDelegationTokenRequestData expireData =
                    new ExpireDelegationTokenRequestData()
                            .setHmac(token.hmac())
                            .setExpiryTimePeriodMs(-1);
            ClientRequest expireRequest = client.newClientRequest(
                    node.idString(),
                    new ExpireDelegationTokenRequest.Builder(expireData),
                    time.milliseconds(),
                    true);
            ExpireDelegationTokenResponse expireResponse =
                    (ExpireDelegationTokenResponse)
                            NetworkClientUtils.sendAndReceive(
                                            client, expireRequest, time)
                                    .responseBody();
            if (expireResponse.error()
                            != Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
                    || expireResponse.expiryTimestamp() != -1) {
                throw new AssertionError(
                        "ExpireDelegationToken error fields diverged: "
                                + expireResponse.data());
            }
        } finally {
            client.close();
            metrics.close();
        }
    }

    private static void delegationTokenRequesterSmoke(
            Properties common, String mechanism, String topic) throws Exception {
        KafkaPrincipal owner = new KafkaPrincipal("User", topic + "-owner");
        KafkaPrincipal requester = new KafkaPrincipal("User", "alice");
        DelegationToken token;
        try (Admin admin = Admin.create(common)) {
            token = admin.createDelegationToken(
                            new CreateDelegationTokenOptions()
                                    .owner(owner)
                                    .maxLifetimeMs(120_000))
                    .delegationToken()
                    .get(10, TimeUnit.SECONDS);
            if (!token.tokenInfo().owner().equals(owner)
                    || !token.tokenInfo().tokenRequester().equals(requester)) {
                throw new AssertionError(
                        "cross-owner delegation token principals were not preserved " + token);
            }
            boolean requesterCanDescribe = admin.describeDelegationToken()
                    .delegationTokens()
                    .get(10, TimeUnit.SECONDS)
                    .stream()
                    .anyMatch(candidate -> candidate.tokenInfo()
                            .tokenId()
                            .equals(token.tokenInfo().tokenId()));
            if (!requesterCanDescribe) {
                throw new AssertionError(
                        "delegation token requester could not describe its token");
            }
            long renewed = admin.renewDelegationToken(
                            token.hmac(),
                            new RenewDelegationTokenOptions().renewTimePeriodMs(60_000))
                    .expiryTimestamp()
                    .get(10, TimeUnit.SECONDS);
            if (renewed > token.tokenInfo().maxTimestamp()) {
                throw new AssertionError(
                        "delegation token requester renewed beyond max lifetime");
            }
        }

        Properties viewer = userProperties(common, "dynamic", "dynamic-secret");
        try (Admin admin = Admin.create(viewer)) {
            boolean aclCanDescribe = admin.describeDelegationToken()
                    .delegationTokens()
                    .get(10, TimeUnit.SECONDS)
                    .stream()
                    .anyMatch(candidate -> candidate.tokenInfo()
                            .tokenId()
                            .equals(token.tokenInfo().tokenId()));
            if (!aclCanDescribe) {
                throw new AssertionError(
                        "DESCRIBE_TOKENS ACL on the full owner principal was ignored");
            }
        }

        try (Admin admin = Admin.create(common)) {
            admin.expireDelegationToken(
                            token.hmac(),
                            new ExpireDelegationTokenOptions().expiryTimePeriodMs(-1))
                    .expiryTimestamp()
                    .get(10, TimeUnit.SECONDS);
        }
        System.out.printf(
                "%s cross-owner delegation token requester and User ACL semantics passed%n",
                mechanism);
    }

    private static AclBinding binding(
            ResourceType resourceType,
            String resourceName,
            String principal,
            AclOperation operation) {
        return new AclBinding(
                new ResourcePattern(resourceType, resourceName, PatternType.LITERAL),
                new AccessControlEntry(
                        principal,
                        "*",
                        operation,
                        AclPermissionType.ALLOW));
    }

    private static Properties securityProperties(
            String bootstrap,
            String certificate,
            String mechanism,
            String username,
            String password) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("ssl.truststore.type", "PEM");
        properties.put("ssl.truststore.certificates", certificate);
        properties.put("ssl.endpoint.identification.algorithm", "https");
        properties.put("sasl.mechanism", mechanism);
        properties.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                        + "username=\"" + escape(username) + "\" "
                        + "password=\"" + escape(password) + "\";");
        return properties;
    }

    private static Properties tokenProperties(
            Properties common, String mechanism, DelegationToken token) {
        Properties properties = new Properties();
        properties.putAll(common);
        properties.put("sasl.mechanism", mechanism);
        properties.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                        + "username=\"" + escape(token.tokenInfo().tokenId()) + "\" "
                        + "password=\"" + escape(token.hmacAsBase64String()) + "\" "
                        + "tokenauth=\"true\";");
        return properties;
    }

    private static Properties userProperties(
            Properties common, String username, String password) {
        Properties properties = new Properties();
        properties.putAll(common);
        properties.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                        + "username=\"" + escape(username) + "\" "
                        + "password=\"" + escape(password) + "\";");
        return properties;
    }

    private static Properties transactionalProducerProperties(
            Properties common, String transactionalId) {
        Properties properties = new Properties();
        properties.putAll(common);
        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000");
        return properties;
    }

    private static void createTopic(Properties common, String topic) throws Exception {
        Properties properties = new Properties();
        properties.putAll(common);
        try (Admin admin = Admin.create(properties)) {
            try {
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                        .all()
                        .get(10, TimeUnit.SECONDS);
            } catch (ExecutionException error) {
                if (!(error.getCause() instanceof TopicExistsException)) {
                    throw error;
                }
            }
        }
    }

    private static void produce(Properties common, String topic) throws Exception {
        Properties properties = new Properties();
        properties.putAll(common);
        properties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            producer.send(new ProducerRecord<>(topic, 0, "secure-key", "secure-value"))
                    .get(10, TimeUnit.SECONDS);
        }
    }

    private static void consume(Properties common, String topic) {
        Properties properties = new Properties();
        properties.putAll(common);
        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, topic + "-group");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put("group.protocol", "classic");

        TopicPartition partition = new TopicPartition(topic, 0);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (System.nanoTime() < deadline) {
                boolean found = consumer.poll(Duration.ofMillis(250))
                        .records(partition)
                        .stream()
                        .anyMatch(record -> "secure-value".equals(record.value()));
                if (found) {
                    return;
                }
            }
        }
        throw new AssertionError("authenticated consumer did not receive the produced record");
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
