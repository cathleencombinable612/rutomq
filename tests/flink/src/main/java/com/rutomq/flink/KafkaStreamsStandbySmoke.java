package com.rutomq.flink;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.StreamsGroupDescription;
import org.apache.kafka.clients.admin.StreamsGroupMemberDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;

public final class KafkaStreamsStandbySmoke {
    private KafkaStreamsStandbySmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException(
                    "KafkaStreamsStandbySmoke <bootstrap> <input> <output> <application-id> <records>");
        }
        String bootstrap = args[0];
        String input = args[1];
        String output = args[2];
        String applicationId = args[3];
        int records = Integer.parseInt(args[4]);
        int additional = Math.max(20, records / 4);
        Path firstState = Files.createTempDirectory("rutomq-streams-standby-a-");
        Path secondState = Files.createTempDirectory("rutomq-streams-standby-b-");
        KafkaStreams first = null;
        KafkaStreams second = null;
        try {
            KafkaStreamsSmoke.createTopics(bootstrap, input, output);
            configureGroup(bootstrap, applicationId);
            first =
                    KafkaStreamsStatefulSmoke.start(
                            bootstrap,
                            input,
                            output,
                            applicationId,
                            firstState,
                            false,
                            1,
                            applicationId + "-process-a");
            second =
                    KafkaStreamsStatefulSmoke.start(
                            bootstrap,
                            input,
                            output,
                            applicationId,
                            secondState,
                            false,
                            1,
                            applicationId + "-process-b");
            StreamsGroupDescription balanced =
                    awaitAssignment(bootstrap, applicationId, 2, 1, 1);
            assertDifferentProcessesOwnActiveAndStandby(balanced);

            KafkaStreamsStatefulSmoke.produce(bootstrap, input, 0, records);
            KafkaStreamsStatefulSmoke.awaitCounts(
                    bootstrap,
                    output,
                    KafkaStreamsStatefulSmoke.expectedCounts(records),
                    Duration.ofSeconds(60));
            KafkaStreamsSmoke.assertHealthy(first, second);

            KafkaStreamsSmoke.close(first);
            first = null;
            awaitAssignment(bootstrap, applicationId, 1, 2, 0);
            KafkaStreamsStatefulSmoke.produce(
                    bootstrap, input, records, additional);
            KafkaStreamsStatefulSmoke.awaitCounts(
                    bootstrap,
                    output,
                    KafkaStreamsStatefulSmoke.expectedCounts(records + additional),
                    Duration.ofSeconds(60));
            KafkaStreamsSmoke.assertHealthy(second);
        } finally {
            if (first != null) {
                KafkaStreamsSmoke.quietClose(first);
            }
            if (second != null) {
                KafkaStreamsSmoke.quietClose(second);
            }
            Utils.delete(firstState.toFile());
            Utils.delete(secondState.toFile());
        }
        System.out.printf(
                "Kafka Streams 4.2 GROUP config and standby failover passed app=%s records=%d promoted=%d%n",
                applicationId,
                records,
                additional);
    }

    private static void configureGroup(String bootstrap, String applicationId)
            throws Exception {
        ConfigResource resource =
                new ConfigResource(ConfigResource.Type.GROUP, applicationId);
        List<AlterConfigOp> changes =
                List.of(
                        set("streams.heartbeat.interval.ms", "5000"),
                        set("streams.session.timeout.ms", "45000"),
                        set("streams.initial.rebalance.delay.ms", "1000"),
                        set("streams.num.standby.replicas", "1"));
        try (Admin admin = Admin.create(KafkaStreamsSmoke.adminProperties(bootstrap))) {
            admin.incrementalAlterConfigs(Map.of(resource, changes))
                    .all()
                    .get(15, TimeUnit.SECONDS);
            Config config =
                    admin.describeConfigs(
                                    List.of(resource),
                                    new DescribeConfigsOptions().includeSynonyms(true))
                            .all()
                            .get(15, TimeUnit.SECONDS)
                            .get(resource);
            ConfigEntry standby = config.get("streams.num.standby.replicas");
            if (standby == null
                    || !"1".equals(standby.value())
                    || standby.source()
                            != ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG) {
                throw new AssertionError(
                        "GROUP config did not round-trip with dynamic source: "
                                + standby);
            }
            if (standby.synonyms().size() != 1
                    || !"streams.num.standby.replicas"
                            .equals(standby.synonyms().get(0).name())
                    || !"1".equals(standby.synonyms().get(0).value())
                    || standby.synonyms().get(0).source()
                            != ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG) {
                throw new AssertionError(
                        "GROUP config synonym chain was not preserved: "
                                + standby.synonyms());
            }
            if (!admin.listConfigResources(
                            Set.of(ConfigResource.Type.GROUP),
                            new org.apache.kafka.clients.admin.ListConfigResourcesOptions())
                    .all()
                    .get(15, TimeUnit.SECONDS)
                    .contains(resource)) {
                throw new AssertionError("GROUP config resource was not listed");
            }
        }
    }

    private static AlterConfigOp set(String name, String value) {
        return new AlterConfigOp(
                new ConfigEntry(name, value), AlterConfigOp.OpType.SET);
    }

    private static StreamsGroupDescription awaitAssignment(
            String bootstrap,
            String applicationId,
            int members,
            int activePerMember,
            int standbyPerMember)
            throws Exception {
        try (Admin admin = Admin.create(KafkaStreamsSmoke.adminProperties(bootstrap))) {
            long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
            while (System.nanoTime() < deadline) {
                try {
                    StreamsGroupDescription description =
                            admin.describeStreamsGroups(List.of(applicationId))
                                    .describedGroups()
                                    .get(applicationId)
                                    .get(10, TimeUnit.SECONDS);
                    if (description.groupState() == GroupState.STABLE
                            && description.members().size() == members
                            && description.members().stream()
                                    .allMatch(
                                            member ->
                                                    taskCount(
                                                                    member.assignment()
                                                                            .activeTasks())
                                                            == activePerMember
                                                    && taskCount(
                                                                    member.assignment()
                                                                            .standbyTasks())
                                                            == standbyPerMember)) {
                        return description;
                    }
                } catch (Exception ignored) {
                    // The cooperative assignment may still be converging.
                }
                Thread.sleep(200);
            }
        }
        throw new AssertionError(
                "streams standby assignment did not converge for " + applicationId);
    }

    private static int taskCount(
            List<org.apache.kafka.clients.admin.StreamsGroupMemberAssignment.TaskIds>
                    tasks) {
        return tasks.stream().mapToInt(task -> task.partitions().size()).sum();
    }

    private static void assertDifferentProcessesOwnActiveAndStandby(
            StreamsGroupDescription description) {
        Map<String, String> activeOwner = new HashMap<>();
        for (StreamsGroupMemberDescription member : description.members()) {
            for (String task : tasks(member.assignment().activeTasks())) {
                activeOwner.put(task, member.processId());
            }
        }
        Set<String> standbyTasks = new HashSet<>();
        for (StreamsGroupMemberDescription member : description.members()) {
            for (String task : tasks(member.assignment().standbyTasks())) {
                standbyTasks.add(task);
                if (member.processId().equals(activeOwner.get(task))) {
                    throw new AssertionError(
                            "active and standby task share process " + task);
                }
            }
        }
        if (!standbyTasks.equals(activeOwner.keySet())) {
            throw new AssertionError(
                    "standby coverage differs active="
                            + activeOwner.keySet()
                            + " standby="
                            + standbyTasks);
        }
    }

    private static List<String> tasks(
            List<org.apache.kafka.clients.admin.StreamsGroupMemberAssignment.TaskIds>
                    assignments) {
        List<String> tasks = new ArrayList<>();
        for (var assignment : assignments) {
            for (int partition : assignment.partitions()) {
                tasks.add(assignment.subtopologyId() + "_" + partition);
            }
        }
        return tasks;
    }
}
