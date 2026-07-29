package com.rutomq.flink;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.StreamsGroupDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

public final class KafkaStreamsSmoke {
    static final int PARTITIONS = 2;
    private static final Duration START_TIMEOUT = Duration.ofSeconds(90);

    private KafkaStreamsSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5 && args.length != 6) {
            throw new IllegalArgumentException(
                    "KafkaStreamsSmoke <bootstrap> <input> <output> <application-id> <records> [classic|streams]");
        }
        String bootstrap = args[0];
        String input = args[1];
        String output = args[2];
        String applicationId = args[3];
        int records = Integer.parseInt(args[4]);
        String groupProtocol = args.length == 6 ? args[5] : "classic";
        if (!groupProtocol.equals("classic") && !groupProtocol.equals("streams")) {
            throw new IllegalArgumentException("unsupported group protocol " + groupProtocol);
        }
        Path stateDirectory = Files.createTempDirectory("rutomq-streams-");

        try {
            createTopics(bootstrap, input, output);
            KafkaStreams first =
                    start(
                            bootstrap,
                            input,
                            output,
                            applicationId,
                            stateDirectory,
                            groupProtocol);
            if (groupProtocol.equals("streams")) {
                awaitStreamsGroup(bootstrap, applicationId, 2);
            }
            produce(bootstrap, input, records);
            assertOutput(bootstrap, output, records, Duration.ofSeconds(60));
            assertHealthy(first);
            close(first);
            first.cleanUp();

            KafkaStreams restarted =
                    start(
                            bootstrap,
                            input,
                            output,
                            applicationId,
                            stateDirectory,
                            groupProtocol);
            if (groupProtocol.equals("streams")) {
                awaitStreamsGroup(bootstrap, applicationId, 2);
            }
            Thread.sleep(3_000);
            assertHealthy(restarted);
            close(restarted);
            assertOutput(bootstrap, output, records, Duration.ofSeconds(10));
        } finally {
            Utils.delete(stateDirectory.toFile());
        }
        System.out.printf(
                "Kafka Streams 4.2 %s exactly_once_v2 passed app=%s records=%d%n",
                groupProtocol,
                applicationId,
                records);
    }

    private static KafkaStreams start(
            String bootstrap,
            String input,
            String output,
            String applicationId,
            Path stateDirectory,
            String groupProtocol)
            throws Exception {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(
                        input,
                        Consumed.with(
                                org.apache.kafka.common.serialization.Serdes.String(),
                                org.apache.kafka.common.serialization.Serdes.String()))
                .mapValues(value -> "streams:" + value)
                .to(
                        output,
                        Produced.with(
                                org.apache.kafka.common.serialization.Serdes.String(),
                                org.apache.kafka.common.serialization.Serdes.String()));

        CountDownLatch running = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        KafkaStreams streams =
                new KafkaStreams(
                        builder.build(),
                        streamsProperties(
                                bootstrap, applicationId, stateDirectory, groupProtocol));
        streams.setStateListener(
                (next, previous) -> {
                    System.out.printf("Kafka Streams state %s -> %s%n", previous, next);
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
        if (!running.await(START_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
            KafkaStreams.State state = streams.state();
            Throwable cause = failure.get();
            quietClose(streams);
            throw new AssertionError(
                    "Kafka Streams did not reach RUNNING, state=" + state, cause);
        }
        if (failure.get() != null) {
            quietClose(streams);
            throw new AssertionError("Kafka Streams startup failed", failure.get());
        }
        return streams;
    }

    static void close(KafkaStreams streams) {
        if (!streams.close(Duration.ofSeconds(15))) {
            throw new AssertionError("Kafka Streams did not close cleanly");
        }
    }

    static void quietClose(KafkaStreams streams) {
        streams.close(Duration.ofSeconds(2));
    }

    static void assertHealthy(KafkaStreams... instances) {
        for (KafkaStreams instance : instances) {
            KafkaStreams.State state = instance.state();
            if (state == KafkaStreams.State.PENDING_ERROR
                    || state == KafkaStreams.State.ERROR) {
                throw new AssertionError(
                        "Kafka Streams instance entered an error state: " + state);
            }
        }
    }

    static void createTopics(String bootstrap, String input, String output)
            throws Exception {
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            try {
                admin.createTopics(
                                List.of(
                                        new NewTopic(input, PARTITIONS, (short) 1),
                                        new NewTopic(output, PARTITIONS, (short) 1)))
                        .all()
                        .get(15, TimeUnit.SECONDS);
            } catch (ExecutionException error) {
                if (!(error.getCause() instanceof TopicExistsException)) {
                    throw error;
                }
            }
        }
    }

    static void awaitStreamsGroup(
            String bootstrap, String applicationId, int expectedMembers) throws Exception {
        awaitStreamsGroup(bootstrap, applicationId, expectedMembers, 1);
    }

    static void awaitStreamsGroup(
            String bootstrap,
            String applicationId,
            int expectedMembers,
            int expectedSubtopologies)
            throws Exception {
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            StreamsGroupDescription last = null;
            while (System.nanoTime() < deadline) {
                try {
                    last = admin.describeStreamsGroups(List.of(applicationId))
                            .describedGroups()
                            .get(applicationId)
                            .get(10, TimeUnit.SECONDS);
                    if (last.groupState() == GroupState.STABLE
                            && last.members().size() == expectedMembers
                            && last.subtopologies().size() == expectedSubtopologies) {
                        boolean listed = admin.listGroups(
                                        new ListGroupsOptions()
                                                .withTypes(Set.of(GroupType.STREAMS)))
                                .all()
                                .get(10, TimeUnit.SECONDS)
                                .stream()
                                .anyMatch(group -> group.groupId().equals(applicationId));
                        if (!listed) {
                            throw new AssertionError(
                                    "streams group missing from type-filtered ListGroups");
                        }
                        return;
                    }
                } catch (Exception ignored) {
                    // The group may still be reconciling.
                }
                Thread.sleep(200);
            }
            throw new AssertionError(
                    "streams group did not become stable: "
                            + (last == null
                                    ? "no description"
                                    : "state="
                                            + last.groupState()
                                            + " members="
                                            + last.members().size()
                                            + " subtopologies="
                                            + last.subtopologies().size()));
        }
    }

    private static void produce(String bootstrap, String topic, int records) throws Exception {
        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(producerProperties(bootstrap))) {
            for (int index = 0; index < records; index++) {
                producer.send(
                        new ProducerRecord<>(
                                topic,
                                index % PARTITIONS,
                                Integer.toString(index),
                                "input-" + index));
            }
            producer.flush();
        }
    }

    private static void assertOutput(
            String bootstrap, String topic, int records, Duration timeout) {
        Set<String> expected = new HashSet<>();
        for (int index = 0; index < records; index++) {
            expected.add("streams:input-" + index);
        }
        List<String> actual = readAll(bootstrap, topic, records, timeout);
        Set<String> unique = new HashSet<>(actual);
        if (actual.size() != records || !unique.equals(expected)) {
            Set<String> missing = new HashSet<>(expected);
            missing.removeAll(unique);
            throw new AssertionError(
                    "Kafka Streams output mismatch count="
                            + actual.size()
                            + " unique="
                            + unique.size()
                            + " missing="
                            + missing);
        }
    }

    private static List<String> readAll(
            String bootstrap, String topic, int records, Duration timeout) {
        List<TopicPartition> partitions = new ArrayList<>();
        for (int partition = 0; partition < PARTITIONS; partition++) {
            partitions.add(new TopicPartition(topic, partition));
        }
        List<String> values = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(consumerProperties(bootstrap))) {
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline && values.size() < records + 1) {
                for (ConsumerRecord<String, String> record :
                        consumer.poll(Duration.ofMillis(250))) {
                    values.add(record.value());
                }
                if (values.size() == records) {
                    Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);
                    boolean caughtUp = partitions.stream()
                            .allMatch(
                                    partition ->
                                            consumer.position(partition)
                                                    >= ends.get(partition));
                    if (caughtUp) {
                        break;
                    }
                }
            }
        }
        return values;
    }

    static Properties streamsProperties(
            String bootstrap,
            String applicationId,
            Path stateDirectory,
            String groupProtocol) {
        return streamsProperties(
                bootstrap, applicationId, stateDirectory, groupProtocol, 2);
    }

    static Properties streamsProperties(
            String bootstrap,
            String applicationId,
            Path stateDirectory,
            String groupProtocol,
            int streamThreads) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, groupProtocol);
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Integer.toString(streamThreads));
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
        properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1");
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.toString());
        properties.put(
                StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
                "earliest");
        if (groupProtocol.equals("classic")) {
            properties.put(
                    StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                    "10000");
            properties.put(
                    StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG),
                    "3000");
        }
        properties.put(
                StreamsConfig.consumerPrefix(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                "10000");
        properties.put(
                StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                "10000");
        properties.put(
                StreamsConfig.producerPrefix(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG),
                "30000");
        return properties;
    }

    static Properties adminProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        return properties;
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
                "streams-verifier-" + System.nanoTime());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        return properties;
    }
}
