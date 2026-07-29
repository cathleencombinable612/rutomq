package com.rutomq.flink;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public final class MultiAgentDurabilitySmoke {
    private MultiAgentDurabilitySmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            throw new IllegalArgumentException(
                    "MultiAgentDurabilitySmoke <prepare|produce|expect-failure|verify> "
                            + "<bootstrap> <topic> ...");
        }
        switch (args[0]) {
            case "prepare" -> prepare(args[1], args[2]);
            case "produce" -> produce(
                    args[1],
                    args[2],
                    args[3],
                    Integer.parseInt(args[4]),
                    Integer.parseInt(args[5]),
                    Integer.parseInt(args[6]),
                    Long.parseLong(args[7]));
            case "expect-failure" -> expectFailure(args[1], args[2], args[3]);
            case "verify" ->
                    verify(args[1], args[2], args[3], Integer.parseInt(args[4]));
            default -> throw new IllegalArgumentException("unknown mode " + args[0]);
        }
    }

    private static void prepare(String bootstrap, String topic) throws Exception {
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(bootstrap))) {
            try {
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                        .all()
                        .get(15, TimeUnit.SECONDS);
            } catch (ExecutionException error) {
                if (!(error.getCause() instanceof TopicExistsException)) {
                    throw error;
                }
            }
        }
        System.out.printf("prepared multi-Agent topic=%s%n", topic);
    }

    private static void produce(
            String bootstrap,
            String topic,
            String prefix,
            int start,
            int count,
            int workers,
            long delayMs)
            throws Exception {
        AtomicInteger next = new AtomicInteger(start);
        int end = start + count;
        var executor = Executors.newFixedThreadPool(workers);
        try {
            List<java.util.concurrent.Future<?>> tasks = new ArrayList<>();
            for (int worker = 0; worker < workers; worker++) {
                int workerId = worker;
                tasks.add(executor.submit(() -> produceWorker(
                        bootstrap, topic, prefix, next, end, workerId, delayMs)));
            }
            for (var task : tasks) {
                task.get(4, TimeUnit.MINUTES);
            }
        } finally {
            executor.shutdownNow();
        }
        System.out.printf(
                "produced multi-Agent topic=%s range=%d..%d workers=%d%n",
                topic, start, end - 1, workers);
    }

    private static void produceWorker(
            String bootstrap,
            String topic,
            String prefix,
            AtomicInteger next,
            int end,
            int worker,
            long delayMs) {
        Properties properties = producerProperties(bootstrap, 10_000, 120_000);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "multi-agent-" + worker);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
            while (true) {
                int index = next.getAndIncrement();
                if (index >= end) {
                    return;
                }
                byte[] value = value(prefix, index);
                try {
                    producer.send(new ProducerRecord<>(topic, 0, null, value))
                            .get(120, TimeUnit.SECONDS);
                    if (delayMs > 0) {
                        Thread.sleep(delayMs);
                    }
                } catch (Exception error) {
                    throw new RuntimeException("produce failed for index " + index, error);
                }
            }
        }
    }

    private static void expectFailure(String bootstrap, String topic, String label)
            throws Exception {
        Properties properties = producerProperties(bootstrap, 1_500, 5_000);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
            try {
                producer.send(new ProducerRecord<>(topic, 0, null, value(label, -1)))
                        .get(15, TimeUnit.SECONDS);
            } catch (ExecutionException error) {
                System.out.printf(
                        "observed expected Produce failure label=%s cause=%s%n",
                        label, error.getCause().getClass().getSimpleName());
                return;
            }
        }
        throw new AssertionError("Produce unexpectedly succeeded for " + label);
    }

    private static void verify(String bootstrap, String topic, String prefix, int expected)
            throws Exception {
        TopicPartition partition = new TopicPartition(topic, 0);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            long endOffset = consumer.endOffsets(List.of(partition)).get(partition);
            if (endOffset != expected) {
                throw new AssertionError(
                        "expected high watermark " + expected + ", got " + endOffset);
            }
            consumer.seekToBeginning(List.of(partition));
            List<Long> offsets = new ArrayList<>();
            Set<String> values = new HashSet<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (offsets.size() < expected && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(250)).forEach(record -> {
                    offsets.add(record.offset());
                    values.add(new String(record.value(), StandardCharsets.UTF_8));
                });
            }
            if (offsets.size() != expected || values.size() != expected) {
                throw new AssertionError(
                        "expected "
                                + expected
                                + " unique records, got offsets="
                                + offsets.size()
                                + " values="
                                + values.size());
            }
            for (int index = 0; index < expected; index++) {
                if (offsets.get(index) != index || !values.contains(prefix + "-" + index)) {
                    throw new AssertionError("missing offset/value at " + index);
                }
            }
        }
        System.out.printf(
                "verified multi-Agent topic=%s contiguousOffsets=0..%d uniqueValues=%d%n",
                topic, expected - 1, expected);
    }

    private static Properties producerProperties(
            String bootstrap, int requestTimeoutMs, int deliveryTimeoutMs) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100");
        properties.put(ProducerConfig.RETRY_BACKOFF_MAX_MS_CONFIG, "1000");
        return properties;
    }

    private static byte[] value(String prefix, int index) {
        return (prefix + "-" + index).getBytes(StandardCharsets.UTF_8);
    }
}
