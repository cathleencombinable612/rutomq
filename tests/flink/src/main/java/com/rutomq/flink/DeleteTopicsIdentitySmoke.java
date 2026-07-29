package com.rutomq.flink;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicIdException;

public final class DeleteTopicsIdentitySmoke {
    private DeleteTopicsIdentitySmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "DeleteTopicsIdentitySmoke <bootstrap> <topic-prefix>");
        }
        String byIdName = args[1] + "-id";
        String byName = args[1] + "-name";
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(args[0]))) {
            admin.createTopics(List.of(
                            new NewTopic(byIdName, 2, (short) 1),
                            new NewTopic(byName, 1, (short) 1)))
                    .all()
                    .get(15, TimeUnit.SECONDS);
            Map<String, TopicListing> listings =
                    admin.listTopics().namesToListings().get(15, TimeUnit.SECONDS);
            Uuid topicId = listings.get(byIdName).topicId();
            if (topicId == null || Uuid.ZERO_UUID.equals(topicId)) {
                throw new AssertionError("created topic did not expose a topic ID");
            }

            DeleteTopicsResult byId =
                    admin.deleteTopics(TopicCollection.ofTopicIds(List.of(topicId)));
            byId.all().get(15, TimeUnit.SECONDS);
            if (byId.topicIdValues() == null || !byId.topicIdValues().containsKey(topicId)) {
                throw new AssertionError("DeleteTopicsResult did not preserve the topic ID");
            }
            admin.deleteTopics(Set.of(byName)).all().get(15, TimeUnit.SECONDS);
            Set<String> remaining = admin.listTopics().names().get(15, TimeUnit.SECONDS);
            if (remaining.contains(byIdName) || remaining.contains(byName)) {
                throw new AssertionError("deleted topics remained visible: " + remaining);
            }

            Uuid missing = Uuid.randomUuid();
            try {
                admin.deleteTopics(TopicCollection.ofTopicIds(List.of(missing)))
                        .all()
                        .get(15, TimeUnit.SECONDS);
                throw new AssertionError("unknown topic ID deletion succeeded");
            } catch (ExecutionException expected) {
                if (!(expected.getCause() instanceof UnknownTopicIdException)) {
                    throw expected;
                }
            }
        }
        System.out.printf(
                "Kafka DeleteTopics v6 topic ID/name identity passed prefix=%s%n", args[1]);
    }
}
