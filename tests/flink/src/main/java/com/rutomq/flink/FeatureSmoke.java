package com.rutomq.flink;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.SupportedVersionRange;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.common.errors.InvalidUpdateVersionException;

public final class FeatureSmoke {
    private static final String METADATA_VERSION = "metadata.version";
    private static final String TRANSACTION_VERSION = "transaction.version";
    private static final String GROUP_VERSION = "group.version";
    private static final String SHARE_VERSION = "share.version";
    private static final String STREAMS_VERSION = "streams.version";

    private FeatureSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("FeatureSmoke <bootstrap>");
        }
        try (Admin admin = Admin.create(adminProperties(args[0]))) {
            FeatureMetadata initial = describe(admin);
            assertSupported(initial, METADATA_VERSION, (short) 22, (short) 30);
            assertSupported(initial, TRANSACTION_VERSION, (short) 0, (short) 2);
            assertSupported(initial, GROUP_VERSION, (short) 0, (short) 1);
            assertSupported(initial, SHARE_VERSION, (short) 0, (short) 1);
            assertSupported(initial, STREAMS_VERSION, (short) 0, (short) 1);
            assertUnsupported(initial, "kraft.version");
            assertUnsupported(initial, "eligible.leader.replicas.version");
            assertFinalized(initial, METADATA_VERSION, (short) 29);
            assertFinalized(initial, TRANSACTION_VERSION, (short) 2);
            assertFinalized(initial, GROUP_VERSION, (short) 1);
            assertFinalized(initial, SHARE_VERSION, (short) 1);
            assertFinalized(initial, STREAMS_VERSION, (short) 1);
            long initialEpoch = initial.finalizedFeaturesEpoch().orElseThrow();

            update(
                    admin,
                    Map.of(
                            METADATA_VERSION,
                            new FeatureUpdate(
                                    (short) 28, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE)),
                    true);
            assertMetadataUnchanged(describe(admin), initialEpoch, (short) 29);

            Map<String, FeatureUpdate> atomicFailure = new LinkedHashMap<>();
            atomicFailure.put(
                    METADATA_VERSION,
                    new FeatureUpdate((short) 28, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE));
            atomicFailure.put(
                    "unknown.feature",
                    new FeatureUpdate((short) 1, FeatureUpdate.UpgradeType.UPGRADE));
            expectInvalidUpdate(admin, atomicFailure);
            assertMetadataUnchanged(describe(admin), initialEpoch, (short) 29);

            expectInvalidUpdate(
                    admin,
                    Map.of(
                            METADATA_VERSION,
                            new FeatureUpdate(
                                    (short) 28, FeatureUpdate.UpgradeType.UPGRADE)));
            assertMetadataUnchanged(describe(admin), initialEpoch, (short) 29);

            update(
                    admin,
                    Map.of(
                            METADATA_VERSION,
                            new FeatureUpdate(
                                    (short) 28, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE)),
                    false);
            FeatureMetadata downgraded = describe(admin);
            assertFinalized(downgraded, METADATA_VERSION, (short) 28);
            if (downgraded.finalizedFeaturesEpoch().orElseThrow() != initialEpoch + 1) {
                throw new AssertionError("feature epoch did not advance after downgrade");
            }

            update(
                    admin,
                    Map.of(
                            METADATA_VERSION,
                            new FeatureUpdate(
                                    (short) 29, FeatureUpdate.UpgradeType.UPGRADE)),
                    false);
            FeatureMetadata restored = describe(admin);
            assertFinalized(restored, METADATA_VERSION, (short) 29);
            if (restored.finalizedFeaturesEpoch().orElseThrow() != initialEpoch + 2) {
                throw new AssertionError("feature epoch did not advance after restore");
            }

            update(
                    admin,
                    Map.of(
                            TRANSACTION_VERSION,
                            new FeatureUpdate(
                                    (short) 0, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                            STREAMS_VERSION,
                            new FeatureUpdate(
                                    (short) 0, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE),
                            SHARE_VERSION,
                            new FeatureUpdate(
                                    (short) 0, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE)),
                    false);
            FeatureMetadata disabled = describe(admin);
            for (String name :
                    new String[] {TRANSACTION_VERSION, SHARE_VERSION, STREAMS_VERSION}) {
                if (disabled.finalizedFeatures().containsKey(name)) {
                    throw new AssertionError(name + " remained finalized after downgrade");
                }
            }
            update(
                    admin,
                    Map.of(
                            TRANSACTION_VERSION,
                            new FeatureUpdate((short) 2, FeatureUpdate.UpgradeType.UPGRADE),
                            STREAMS_VERSION,
                            new FeatureUpdate((short) 1, FeatureUpdate.UpgradeType.UPGRADE),
                            SHARE_VERSION,
                            new FeatureUpdate((short) 1, FeatureUpdate.UpgradeType.UPGRADE)),
                    false);
            assertFinalized(describe(admin), SHARE_VERSION, (short) 1);
            assertFinalized(describe(admin), TRANSACTION_VERSION, (short) 2);
            assertFinalized(describe(admin), STREAMS_VERSION, (short) 1);
        }
        System.out.println("Kafka Admin feature describe/update acceptance passed");
    }

    private static FeatureMetadata describe(Admin admin) throws Exception {
        return admin.describeFeatures().featureMetadata().get(10, TimeUnit.SECONDS);
    }

    private static void update(
            Admin admin, Map<String, FeatureUpdate> updates, boolean validateOnly)
            throws Exception {
        admin.updateFeatures(updates, new UpdateFeaturesOptions().validateOnly(validateOnly))
                .all()
                .get(10, TimeUnit.SECONDS);
    }

    private static void expectInvalidUpdate(
            Admin admin, Map<String, FeatureUpdate> updates) throws Exception {
        try {
            update(admin, updates, false);
            throw new AssertionError("invalid feature update was accepted");
        } catch (ExecutionException error) {
            if (!(error.getCause() instanceof InvalidUpdateVersionException)) {
                throw error;
            }
        }
    }

    private static void assertSupported(
            FeatureMetadata metadata, String name, short minimum, short maximum) {
        SupportedVersionRange range = metadata.supportedFeatures().get(name);
        if (range == null
                || range.minVersion() != minimum
                || range.maxVersion() != maximum) {
            throw new AssertionError("unexpected supported feature " + name + ": " + range);
        }
    }

    private static void assertFinalized(
            FeatureMetadata metadata, String name, short expected) {
        FinalizedVersionRange range = metadata.finalizedFeatures().get(name);
        if (range == null
                || range.minVersionLevel() != expected
                || range.maxVersionLevel() != expected) {
            throw new AssertionError("unexpected finalized feature " + name + ": " + range);
        }
    }

    private static void assertUnsupported(FeatureMetadata metadata, String name) {
        if (metadata.supportedFeatures().containsKey(name)
                || metadata.finalizedFeatures().containsKey(name)) {
            throw new AssertionError("unsupported physical feature was advertised: " + name);
        }
    }

    private static void assertMetadataUnchanged(
            FeatureMetadata metadata, long epoch, short version) {
        assertFinalized(metadata, METADATA_VERSION, version);
        if (metadata.finalizedFeaturesEpoch().orElseThrow() != epoch) {
            throw new AssertionError("validate/failed update changed feature epoch");
        }
    }

    private static Properties adminProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        return properties;
    }
}
