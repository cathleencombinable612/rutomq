package com.rutomq.flink;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;

public final class CreateTopicsIdentitySmoke {
    private CreateTopicsIdentitySmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "CreateTopicsIdentitySmoke <bootstrap> <topic-prefix>");
        }
        String prefix = args[1];
        String existing = prefix + "-existing";
        String collisionSource = prefix + "_collision";
        String collision = prefix + ".collision";
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(args[0]))) {
            admin.createTopics(List.of(
                            new NewTopic(existing, 1, (short) 1),
                            new NewTopic(collisionSource, 1, (short) 1)))
                    .all()
                    .get(15, TimeUnit.SECONDS);

            reject(
                    admin,
                    new NewTopic(existing, 1, (short) 1),
                    new CreateTopicsOptions().validateOnly(true),
                    TopicExistsException.class);
            reject(
                    admin,
                    new NewTopic(existing, 1, (short) 1),
                    new CreateTopicsOptions(),
                    TopicExistsException.class);
            reject(
                    admin,
                    new NewTopic(collision, 1, (short) 1),
                    new CreateTopicsOptions(),
                    InvalidTopicException.class);
            reject(
                    admin,
                    new NewTopic(prefix + "/invalid", 1, (short) 1),
                    new CreateTopicsOptions(),
                    InvalidTopicException.class);
            reject(
                    admin,
                    new NewTopic("a".repeat(250), 1, (short) 1),
                    new CreateTopicsOptions(),
                    InvalidTopicException.class);

            var names = admin.listTopics().names().get(15, TimeUnit.SECONDS);
            if (names.contains(collision) || names.contains(prefix + "/invalid")) {
                throw new AssertionError("an invalid or colliding topic was persisted");
            }
        }
        System.out.printf("Kafka CreateTopics identity validation passed prefix=%s%n", prefix);
    }

    private static void reject(
            Admin admin,
            NewTopic topic,
            CreateTopicsOptions options,
            Class<? extends Throwable> expected)
            throws Exception {
        try {
            admin.createTopics(List.of(topic), options)
                    .all()
                    .get(15, TimeUnit.SECONDS);
            throw new AssertionError("invalid topic was accepted: " + topic.name());
        } catch (ExecutionException error) {
            if (!expected.isInstance(error.getCause())) {
                throw new AssertionError(
                        "unexpected error for " + topic.name() + ": " + error.getCause(),
                        error);
            }
        }
    }
}
