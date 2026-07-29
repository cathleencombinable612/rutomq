package com.rutomq.flink;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;

public final class FileDeleteDelaySmoke {
    private static final String CONFIG = "file.delete.delay.ms";

    private FileDeleteDelaySmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("FileDeleteDelaySmoke <bootstrap> <topic>");
        }
        String bootstrap = args[0];
        String topic = args[1];
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(bootstrap))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)
                            .configs(Map.of(CONFIG, "123"))))
                    .all()
                    .get(15, TimeUnit.SECONDS);
            assertValue(admin, resource, "123");

            try {
                alter(admin, resource, new ConfigEntry(CONFIG, "-1"), AlterConfigOp.OpType.SET);
                throw new AssertionError("negative file.delete.delay.ms was accepted");
            } catch (ExecutionException expected) {
                // The broker returns INVALID_REQUEST through the Admin future.
            }
            assertValue(admin, resource, "123");

            alter(admin, resource, new ConfigEntry(CONFIG, "456"), AlterConfigOp.OpType.SET);
            assertValue(admin, resource, "456");
            alter(admin, resource, new ConfigEntry(CONFIG, null), AlterConfigOp.OpType.DELETE);
            assertValue(admin, resource, "60000");
        }
        System.out.printf("Kafka file.delete.delay.ms passed topic=%s default=60000%n", topic);
    }

    private static void alter(
            Admin admin,
            ConfigResource resource,
            ConfigEntry entry,
            AlterConfigOp.OpType operation)
            throws Exception {
        admin.incrementalAlterConfigs(
                        Map.of(resource, List.of(new AlterConfigOp(entry, operation))))
                .all()
                .get(15, TimeUnit.SECONDS);
    }

    private static void assertValue(Admin admin, ConfigResource resource, String expected)
            throws Exception {
        Config config = admin.describeConfigs(List.of(resource))
                .all()
                .get(15, TimeUnit.SECONDS)
                .get(resource);
        ConfigEntry entry = config.get(CONFIG);
        if (entry == null || !expected.equals(entry.value())) {
            throw new AssertionError(
                    "unexpected "
                            + CONFIG
                            + ": "
                            + (entry == null ? null : entry.value()));
        }
    }
}
