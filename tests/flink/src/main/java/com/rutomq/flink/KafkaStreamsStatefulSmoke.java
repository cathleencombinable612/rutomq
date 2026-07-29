package com.rutomq.flink;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public final class KafkaStreamsStatefulSmoke {
    private static final int KEYS = 4;
    private static final String STORE = "counts-store";

    private KafkaStreamsStatefulSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5 && args.length != 6) {
            throw new IllegalArgumentException(
                    "KafkaStreamsStatefulSmoke <bootstrap> <input> <output> <application-id> <records> [repartition]");
        }
        String bootstrap = args[0];
        String input = args[1];
        String output = args[2];
        String applicationId = args[3];
        int records = Integer.parseInt(args[4]);
        boolean repartition = args.length == 6 && args[5].equals("repartition");
        int additional = Math.max(20, records / 4);
        Path stateDirectory = Files.createTempDirectory("rutomq-streams-stateful-");

        try {
            KafkaStreamsSmoke.createTopics(bootstrap, input, output);
            KafkaStreams first =
                    start(
                            bootstrap,
                            input,
                            output,
                            applicationId,
                            stateDirectory,
                            repartition);
            KafkaStreamsSmoke.awaitStreamsGroup(
                    bootstrap, applicationId, 2, repartition ? 2 : 1);
            verifyInternalTopics(bootstrap, applicationId, repartition);
            produce(bootstrap, input, 0, records);
            awaitCounts(
                    bootstrap,
                    output,
                    expectedCounts(records),
                    Duration.ofSeconds(60));
            KafkaStreamsSmoke.assertHealthy(first);
            KafkaStreamsSmoke.close(first);
            Utils.delete(stateDirectory.toFile());
            Files.createDirectories(stateDirectory);

            KafkaStreams restarted =
                    start(
                            bootstrap,
                            input,
                            output,
                            applicationId,
                            stateDirectory,
                            repartition);
            KafkaStreamsSmoke.awaitStreamsGroup(
                    bootstrap, applicationId, 2, repartition ? 2 : 1);
            produce(bootstrap, input, records, additional);
            awaitCounts(
                    bootstrap,
                    output,
                    expectedCounts(records + additional),
                    Duration.ofSeconds(60));
            KafkaStreamsSmoke.assertHealthy(restarted);
            KafkaStreamsSmoke.close(restarted);
        } finally {
            Utils.delete(stateDirectory.toFile());
        }
        System.out.printf(
                "Kafka Streams 4.2 stateful streams protocol passed app=%s records=%d restored=%d repartition=%s%n",
                applicationId,
                records,
                additional,
                repartition);
    }

    static KafkaStreams start(
            String bootstrap,
            String input,
            String output,
            String applicationId,
            Path stateDirectory,
            boolean repartition)
            throws Exception {
        return start(
                bootstrap,
                input,
                output,
                applicationId,
                stateDirectory,
                repartition,
                2,
                null);
    }

    static KafkaStreams start(
            String bootstrap,
            String input,
            String output,
            String applicationId,
            Path stateDirectory,
            boolean repartition,
            int streamThreads,
            String clientId)
            throws Exception {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream =
                builder.stream(
                        input,
                        Consumed.with(
                                org.apache.kafka.common.serialization.Serdes.String(),
                                org.apache.kafka.common.serialization.Serdes.String()));
        if (repartition) {
            stream = stream.selectKey((key, value) -> key);
        }
        stream
                .groupByKey(
                        Grouped.with(
                                org.apache.kafka.common.serialization.Serdes.String(),
                                org.apache.kafka.common.serialization.Serdes.String()))
                .count(Materialized.as(STORE))
                .toStream()
                .to(
                        output,
                        Produced.with(
                                org.apache.kafka.common.serialization.Serdes.String(),
                                org.apache.kafka.common.serialization.Serdes.Long()));

        CountDownLatch running = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Properties properties =
                KafkaStreamsSmoke.streamsProperties(
                        bootstrap,
                        applicationId,
                        stateDirectory,
                        "streams",
                        streamThreads);
        if (clientId != null) {
            properties.put(
                    org.apache.kafka.streams.StreamsConfig.CLIENT_ID_CONFIG, clientId);
        }
        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.setStateListener(
                (next, previous) -> {
                    System.out.printf(
                            "Kafka Streams stateful state %s -> %s%n", previous, next);
                    if (next == KafkaStreams.State.RUNNING) {
                        running.countDown();
                    } else if (next == KafkaStreams.State.ERROR) {
                        failure.compareAndSet(
                                null,
                                new IllegalStateException(
                                        "Kafka Streams entered ERROR from " + previous));
                        running.countDown();
                    }
                });
        streams.setUncaughtExceptionHandler(
                error -> {
                    failure.compareAndSet(null, error);
                    running.countDown();
                    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse
                            .SHUTDOWN_CLIENT;
                });
        streams.start();
        if (!running.await(45, TimeUnit.SECONDS)) {
            KafkaStreamsSmoke.quietClose(streams);
            throw new AssertionError(
                    "stateful Kafka Streams did not reach RUNNING, state=" + streams.state());
        }
        if (failure.get() != null) {
            KafkaStreamsSmoke.quietClose(streams);
            throw new AssertionError("stateful Kafka Streams startup failed", failure.get());
        }
        return streams;
    }

    private static void verifyInternalTopics(
            String bootstrap, String applicationId, boolean repartition)
            throws Exception {
        String changelog = applicationId + "-" + STORE + "-changelog";
        try (Admin admin = Admin.create(KafkaStreamsSmoke.adminProperties(bootstrap))) {
            Map<String, org.apache.kafka.clients.admin.TopicDescription> topics =
                    admin.describeTopics(List.of(changelog))
                            .allTopicNames()
                            .get(15, TimeUnit.SECONDS);
            if (topics.get(changelog).partitions().size() != KafkaStreamsSmoke.PARTITIONS) {
                throw new AssertionError("unexpected changelog partitions " + topics);
            }
            ConfigResource resource =
                    new ConfigResource(ConfigResource.Type.TOPIC, changelog);
            String cleanupPolicy =
                    admin.describeConfigs(List.of(resource))
                            .all()
                            .get(15, TimeUnit.SECONDS)
                            .get(resource)
                            .get("cleanup.policy")
                            .value();
            if (!cleanupPolicy.contains("compact")) {
                throw new AssertionError(
                        "changelog is not compacted: " + cleanupPolicy);
            }
            if (repartition) {
                boolean found =
                        admin.listTopics()
                                .names()
                                .get(15, TimeUnit.SECONDS)
                                .stream()
                                .anyMatch(
                                        topic ->
                                                topic.startsWith(applicationId + "-")
                                                        && topic.endsWith("-repartition"));
                if (!found) {
                    throw new AssertionError("repartition topic was not created");
                }
            }
        }
    }

    static void produce(
            String bootstrap, String topic, int start, int count) throws Exception {
        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(producerProperties(bootstrap))) {
            for (int index = start; index < start + count; index++) {
                producer.send(
                        new ProducerRecord<>(
                                topic,
                                index % KafkaStreamsSmoke.PARTITIONS,
                                key(index),
                                "value-" + index));
            }
            producer.flush();
        }
    }

    static void awaitCounts(
            String bootstrap,
            String topic,
            Map<String, Long> expected,
            Duration timeout) {
        List<TopicPartition> partitions = new ArrayList<>();
        for (int partition = 0; partition < KafkaStreamsSmoke.PARTITIONS; partition++) {
            partitions.add(new TopicPartition(topic, partition));
        }
        Map<String, Long> actual = new HashMap<>();
        try (KafkaConsumer<String, Long> consumer =
                new KafkaConsumer<>(consumerProperties(bootstrap))) {
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, Long> record :
                        consumer.poll(Duration.ofMillis(250))) {
                    actual.put(record.key(), record.value());
                }
                if (actual.equals(expected)) {
                    return;
                }
            }
        }
        throw new AssertionError(
                "stateful Kafka Streams counts differ expected="
                        + expected
                        + " actual="
                        + actual);
    }

    static Map<String, Long> expectedCounts(int records) {
        Map<String, Long> counts = new HashMap<>();
        for (int index = 0; index < records; index++) {
            counts.merge(key(index), 1L, Long::sum);
        }
        return counts;
    }

    private static String key(int index) {
        return "key-" + (index % KEYS);
    }

    private static Properties producerProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        return properties;
    }

    private static Properties consumerProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "stateful-streams-verifier-" + System.nanoTime());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        return properties;
    }
}
