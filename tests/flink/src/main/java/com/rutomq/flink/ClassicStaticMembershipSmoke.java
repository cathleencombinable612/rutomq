package com.rutomq.flink;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ClassicGroupDescription;
import org.apache.kafka.clients.admin.MemberToRemove;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.serialization.StringDeserializer;

public final class ClassicStaticMembershipSmoke {
    private static final String INSTANCE_ID = "static-instance-a";

    private ClassicStaticMembershipSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                    "ClassicStaticMembershipSmoke <bootstrap> <topic> <group>");
        }
        String bootstrap = args[0];
        String topic = args[1];
        String group = args[2];
        ConsumerProtocolSmoke.createTopic(bootstrap, topic);

        KafkaConsumer<String, String> first =
                new KafkaConsumer<>(consumerProperties(bootstrap, group, "static-first"));
        KafkaConsumer<String, String> replacement = null;
        try {
            first.subscribe(List.of(topic));
            awaitAssignment(first);
            assertStaticMember(bootstrap, group);

            replacement =
                    new KafkaConsumer<>(consumerProperties(bootstrap, group, "static-replacement"));
            replacement.subscribe(List.of(topic));
            awaitAssignment(replacement);
            awaitFenced(first);
            assertStaticMember(bootstrap, group);
            removeStaticMember(bootstrap, group);
        } finally {
            first.close(Duration.ofSeconds(2));
            if (replacement != null) {
                replacement.close(Duration.ofSeconds(2));
            }
        }
        awaitEmptyGroup(bootstrap, group);
        System.out.printf(
                "Kafka classic static membership passed group=%s instance=%s%n",
                group, INSTANCE_ID);
    }

    private static Properties consumerProperties(
            String bootstrap, String group, String clientId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, INSTANCE_ID);
        properties.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        return properties;
    }

    private static void awaitAssignment(KafkaConsumer<String, String> consumer) {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            consumer.poll(Duration.ofMillis(200));
            if (!consumer.assignment().isEmpty()) {
                return;
            }
        }
        throw new AssertionError("static consumer did not receive an assignment");
    }

    private static void awaitFenced(KafkaConsumer<String, String> consumer) {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            try {
                consumer.poll(Duration.ofMillis(200));
            } catch (RuntimeException error) {
                if (hasFencedCause(error)) {
                    return;
                }
                throw error;
            }
        }
        throw new AssertionError("replaced static member was not fenced");
    }

    private static boolean hasFencedCause(Throwable error) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            if (current instanceof FencedInstanceIdException) {
                return true;
            }
        }
        return false;
    }

    private static void assertStaticMember(String bootstrap, String group) throws Exception {
        ClassicGroupDescription description = describe(bootstrap, group);
        if (description == null
                || description.members().size() != 1
                || description.members().stream()
                        .noneMatch(member -> member.groupInstanceId()
                                .filter(INSTANCE_ID::equals)
                                .isPresent())) {
            throw new AssertionError("unexpected static group description " + description);
        }
    }

    private static void removeStaticMember(String bootstrap, String group) throws Exception {
        MemberToRemove member = new MemberToRemove(INSTANCE_ID);
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(bootstrap))) {
            admin.removeMembersFromConsumerGroup(
                            group,
                            new RemoveMembersFromConsumerGroupOptions(List.of(member)))
                    .memberResult(member)
                    .get(15, TimeUnit.SECONDS);
        }
    }

    private static void awaitEmptyGroup(String bootstrap, String group) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            ClassicGroupDescription description = describe(bootstrap, group);
            if (description == null || description.members().isEmpty()) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("static group did not become empty");
    }

    private static ClassicGroupDescription describe(String bootstrap, String group)
            throws Exception {
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(bootstrap))) {
            try {
                return admin.describeClassicGroups(List.of(group))
                        .describedGroups()
                        .get(group)
                        .get(10, TimeUnit.SECONDS);
            } catch (ExecutionException error) {
                if (error.getCause() instanceof GroupIdNotFoundException) {
                    return null;
                }
                throw error;
            }
        }
    }
}
