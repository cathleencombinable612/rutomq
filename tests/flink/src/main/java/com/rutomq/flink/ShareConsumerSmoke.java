package com.rutomq.flink;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class ShareConsumerSmoke {
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

    private ShareConsumerSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException(
                    "ShareConsumerSmoke <bootstrap> <topic> <group> <records>");
        }
        String bootstrap = args[0];
        String topic = args[1];
        String group = args[2];
        int records = Integer.parseInt(args[3]);
        createTopic(bootstrap, topic);

        AtomicBoolean stop = new AtomicBoolean();
        AtomicBoolean released = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch completed = new CountDownLatch(2);
        CountDownLatch workersReceived = new CountDownLatch(2);
        Set<String> accepted = ConcurrentHashMap.newKeySet();
        Set<String> workersWithRecords = ConcurrentHashMap.newKeySet();
        Map<String, Integer> deliveryCounts = new ConcurrentHashMap<>();
        Thread first = worker(
                "share-a", bootstrap, topic, group, started, completed, stop, released,
                failure, accepted, workersWithRecords, workersReceived, deliveryCounts);
        Thread second = worker(
                "share-b", bootstrap, topic, group, started, completed, stop, released,
                failure, accepted, workersWithRecords, workersReceived, deliveryCounts);
        first.start();
        second.start();

        if (!started.await(15, TimeUnit.SECONDS)) {
            throw new AssertionError("share consumers did not start");
        }
        awaitStableGroup(bootstrap, group, 2);
        produce(bootstrap, topic, records);

        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline
                && accepted.size() < records
                && failure.get() == null) {
            Thread.sleep(100);
        }
        stop.set(true);
        completed.await(15, TimeUnit.SECONDS);
        first.join(5_000);
        second.join(5_000);
        if (failure.get() != null) {
            throw new AssertionError("share consumer worker failed", failure.get());
        }

        Set<String> expected = new HashSet<>();
        for (int index = 0; index < records; index++) {
            expected.add("share-" + index);
        }
        if (!accepted.equals(expected)) {
            Set<String> missing = new HashSet<>(expected);
            missing.removeAll(accepted);
            throw new AssertionError("share consumer missing records " + missing);
        }
        if (deliveryCounts.getOrDefault("share-0", 0) < 2) {
            throw new AssertionError("released record was not redelivered: " + deliveryCounts);
        }
        if (workersWithRecords.size() != 2) {
            throw new AssertionError(
                    "expected both share members to receive records, got " + workersWithRecords);
        }
        awaitEmptyGroup(bootstrap, group);
        verifyOffsetAdministration(bootstrap, topic, group, records);
        System.out.printf(
                "Kafka share consumer/Admin passed group=%s members=2 records=%d releasedDeliveries=%d%n",
                group, records, deliveryCounts.get("share-0"));
    }

    private static Thread worker(
            String workerId,
            String bootstrap,
            String topic,
            String group,
            CountDownLatch started,
            CountDownLatch completed,
            AtomicBoolean stop,
            AtomicBoolean released,
            AtomicReference<Throwable> failure,
            Set<String> accepted,
            Set<String> workersWithRecords,
            CountDownLatch workersReceived,
            Map<String, Integer> deliveryCounts) {
        return new Thread(() -> {
            try (KafkaShareConsumer<String, String> consumer =
                    new KafkaShareConsumer<>(consumerProperties(bootstrap, group, workerId))) {
                consumer.subscribe(List.of(topic));
                started.countDown();
                while (!stop.get() && failure.get() == null) {
                    var batch = consumer.poll(Duration.ofMillis(250));
                    if (!batch.isEmpty() && workersWithRecords.add(workerId)) {
                        workersReceived.countDown();
                        if (!workersReceived.await(15, TimeUnit.SECONDS)) {
                            throw new AssertionError(
                                    "both share members did not acquire records");
                        }
                    }
                    for (ConsumerRecord<String, String> record : batch) {
                        String value = record.value();
                        deliveryCounts.merge(value, 1, Integer::sum);
                        if ("share-0".equals(value) && released.compareAndSet(false, true)) {
                            consumer.acknowledge(record, AcknowledgeType.RELEASE);
                        } else {
                            if (!accepted.add(value)) {
                                throw new AssertionError("accepted duplicate record " + value);
                            }
                            consumer.acknowledge(record, AcknowledgeType.ACCEPT);
                        }
                    }
                    if (!batch.isEmpty()) {
                        assertCommitted(consumer.commitSync(REQUEST_TIMEOUT));
                    }
                }
            } catch (Throwable error) {
                failure.compareAndSet(null, error);
                stop.set(true);
            } finally {
                completed.countDown();
            }
        }, workerId);
    }

    private static Properties consumerProperties(String bootstrap, String group, String clientId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        properties.put(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");
        properties.put(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "record_limit");
        return properties;
    }

    private static void assertCommitted(
            Map<TopicIdPartition, java.util.Optional<org.apache.kafka.common.KafkaException>>
                    results) {
        results.forEach((partition, error) -> error.ifPresent(cause -> {
            throw new AssertionError("share acknowledgement failed for " + partition, cause);
        }));
    }

    private static void awaitStableGroup(String bootstrap, String group, int members)
            throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        ShareGroupDescription last = null;
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            while (System.nanoTime() < deadline) {
                try {
                    last = admin.describeShareGroups(List.of(group))
                            .describedGroups()
                            .get(group)
                            .get(10, TimeUnit.SECONDS);
                    if ("Stable".equals(last.groupState().toString())
                            && last.members().size() == members) {
                        return;
                    }
                } catch (Exception ignored) {
                    // The group may not exist until the first heartbeat completes.
                }
                Thread.sleep(200);
            }
        }
        throw new AssertionError("share group did not become stable: " + last);
    }

    private static void awaitEmptyGroup(String bootstrap, String group) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        ShareGroupDescription last = null;
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            while (System.nanoTime() < deadline) {
                last = admin.describeShareGroups(List.of(group))
                        .describedGroups()
                        .get(group)
                        .get(10, TimeUnit.SECONDS);
                if (last.members().isEmpty()) {
                    return;
                }
                Thread.sleep(200);
            }
        }
        throw new AssertionError("share group did not become empty: " + last);
    }

    private static void verifyOffsetAdministration(
            String bootstrap, String topic, String group, long consumed) throws Exception {
        TopicPartition partition = new TopicPartition(topic, 0);
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            Map<TopicPartition, SharePartitionOffsetInfo> explicit =
                    admin.listShareGroupOffsets(Map.of(
                                    group,
                                    new ListShareGroupOffsetsSpec()
                                            .topicPartitions(List.of(partition))))
                            .partitionsToOffsetInfo(group)
                            .get(10, TimeUnit.SECONDS);
            assertOffset(explicit, partition, consumed, 0L);

            Map<TopicPartition, SharePartitionOffsetInfo> all =
                    admin.listShareGroupOffsets(
                                    Map.of(group, new ListShareGroupOffsetsSpec()))
                            .partitionsToOffsetInfo(group)
                            .get(10, TimeUnit.SECONDS);
            assertOffset(all, partition, consumed, 0L);

            admin.alterShareGroupOffsets(group, Map.of(partition, 0L))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            Map<TopicPartition, SharePartitionOffsetInfo> reset =
                    admin.listShareGroupOffsets(Map.of(
                                    group,
                                    new ListShareGroupOffsetsSpec()
                                            .topicPartitions(List.of(partition))))
                            .partitionsToOffsetInfo(group)
                            .get(10, TimeUnit.SECONDS);
            assertOffset(reset, partition, 0L, consumed);

            admin.deleteShareGroupOffsets(group, Set.of(topic))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            Map<TopicPartition, SharePartitionOffsetInfo> deleted =
                    admin.listShareGroupOffsets(
                                    Map.of(group, new ListShareGroupOffsetsSpec()))
                            .partitionsToOffsetInfo(group)
                            .get(10, TimeUnit.SECONDS);
            if (!deleted.isEmpty()) {
                throw new AssertionError("deleted share offsets remain visible: " + deleted);
            }
            admin.deleteShareGroups(List.of(group)).all().get(10, TimeUnit.SECONDS);
        }
    }

    private static void assertOffset(
            Map<TopicPartition, SharePartitionOffsetInfo> offsets,
            TopicPartition partition,
            long expectedStart,
            long expectedLag) {
        SharePartitionOffsetInfo info = offsets.get(partition);
        if (info == null
                || info.startOffset() != expectedStart
                || info.lag().orElse(-1L) != expectedLag) {
            throw new AssertionError(
                    "unexpected share offset for " + partition + ": " + info);
        }
        if (info.leaderEpoch().orElse(-1) != 0) {
            throw new AssertionError("unexpected share leader epoch: " + info);
        }
    }

    private static void createTopic(String bootstrap, String topic) throws Exception {
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            try {
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                        .all()
                        .get(15, TimeUnit.SECONDS);
            } catch (java.util.concurrent.ExecutionException error) {
                if (!(error.getCause() instanceof TopicExistsException)) {
                    throw error;
                }
            }
        }
    }

    private static void produce(String bootstrap, String topic, int records) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int index = 0; index < records; index++) {
                producer.send(new ProducerRecord<>(
                        topic, 0, Integer.toString(index), "share-" + index));
            }
            producer.flush();
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
