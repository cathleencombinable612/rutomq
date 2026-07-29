package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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

public final class PerformanceSmoke {
    private PerformanceSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            throw new IllegalArgumentException(
                    "PerformanceSmoke <bootstrap> <topic> <records> <payload-bytes> "
                            + "<producers> <partitions>");
        }
        String bootstrap = args[0];
        String topic = args[1];
        int records = Integer.parseInt(args[2]);
        int payloadBytes = Integer.parseInt(args[3]);
        int producers = Integer.parseInt(args[4]);
        int partitions = Integer.parseInt(args[5]);
        if (records <= 0 || payloadBytes < 4 || producers <= 0 || partitions <= 0) {
            throw new IllegalArgumentException("all dimensions must be positive; payload >= 4");
        }

        prepare(bootstrap, topic, partitions);
        long[] latencyNanos =
                produce(bootstrap, topic, records, payloadBytes, producers, partitions);
        verify(bootstrap, topic, records, payloadBytes, partitions);
        Arrays.sort(latencyNanos);
        double durationSeconds = elapsedSeconds;
        double recordsPerSecond = records / durationSeconds;
        double mebibytesPerSecond =
                recordsPerSecond * payloadBytes / (1024.0 * 1024.0);
        System.out.printf(
                Locale.ROOT,
                "{\"records\":%d,\"payloadBytes\":%d,\"producers\":%d,"
                        + "\"partitions\":%d,\"durationMs\":%.3f,"
                        + "\"throughputRecordsPerSecond\":%.3f,"
                        + "\"throughputMiBPerSecond\":%.3f,"
                        + "\"produceLatencyMs\":{\"p50\":%.3f,\"p95\":%.3f,\"p99\":%.3f}}%n",
                records,
                payloadBytes,
                producers,
                partitions,
                durationSeconds * 1000.0,
                recordsPerSecond,
                mebibytesPerSecond,
                percentileMs(latencyNanos, 0.50),
                percentileMs(latencyNanos, 0.95),
                percentileMs(latencyNanos, 0.99));
    }

    private static volatile double elapsedSeconds;

    private static void prepare(String bootstrap, String topic, int partitions)
            throws Exception {
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(bootstrap))) {
            try {
                admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                        .all()
                        .get(30, TimeUnit.SECONDS);
            } catch (java.util.concurrent.ExecutionException error) {
                if (!(error.getCause() instanceof TopicExistsException)) {
                    throw error;
                }
            }
        }
    }

    private static long[] produce(
            String bootstrap,
            String topic,
            int records,
            int payloadBytes,
            int producers,
            int partitions)
            throws Exception {
        long[] latencyNanos = new long[records];
        AtomicInteger next = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        var executor = Executors.newFixedThreadPool(producers);
        var tasks = new ArrayList<java.util.concurrent.Future<?>>();
        long started = System.nanoTime();
        try {
            for (int worker = 0; worker < producers; worker++) {
                int workerId = worker;
                tasks.add(executor.submit(() -> produceWorker(
                        bootstrap,
                        topic,
                        records,
                        payloadBytes,
                        partitions,
                        workerId,
                        next,
                        latencyNanos,
                        failure)));
            }
            for (var task : tasks) {
                task.get(5, TimeUnit.MINUTES);
            }
        } finally {
            executor.shutdownNow();
        }
        elapsedSeconds = (System.nanoTime() - started) / 1_000_000_000.0;
        if (failure.get() != null) {
            throw new RuntimeException("performance Produce failed", failure.get());
        }
        return latencyNanos;
    }

    private static void produceWorker(
            String bootstrap,
            String topic,
            int records,
            int payloadBytes,
            int partitions,
            int worker,
            AtomicInteger next,
            long[] latencyNanos,
            AtomicReference<Throwable> failure) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(64 * 1024));
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "performance-" + worker);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
            while (failure.get() == null) {
                int index = next.getAndIncrement();
                if (index >= records) {
                    break;
                }
                byte[] value = payload(index, payloadBytes);
                long sentAt = System.nanoTime();
                producer.send(
                        new ProducerRecord<>(topic, index % partitions, null, value),
                        (metadata, error) -> {
                            latencyNanos[index] = System.nanoTime() - sentAt;
                            if (error != null) {
                                failure.compareAndSet(null, error);
                            }
                        });
            }
            producer.flush();
        } catch (Throwable error) {
            failure.compareAndSet(null, error);
        }
    }

    private static void verify(
            String bootstrap, String topic, int records, int payloadBytes, int partitions) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000");
        List<TopicPartition> assigned = new ArrayList<>();
        for (int partition = 0; partition < partitions; partition++) {
            assigned.add(new TopicPartition(topic, partition));
        }
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(assigned);
            long highWatermark = consumer.endOffsets(assigned).values().stream()
                    .mapToLong(Long::longValue)
                    .sum();
            if (highWatermark != records) {
                throw new AssertionError(
                        "expected high watermark sum " + records + ", got " + highWatermark);
            }
            for (int pass = 0; pass < 2; pass++) {
                consumer.seekToBeginning(assigned);
                BitSet seen = new BitSet(records);
                long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
                while (seen.cardinality() < records && System.nanoTime() < deadline) {
                    consumer.poll(Duration.ofMillis(250)).forEach(record -> {
                        byte[] value = record.value();
                        if (value.length != payloadBytes) {
                            throw new AssertionError("unexpected payload size " + value.length);
                        }
                        int index = ByteBuffer.wrap(value).getInt();
                        if (index < 0 || index >= records || seen.get(index)) {
                            throw new AssertionError("duplicate or invalid record " + index);
                        }
                        seen.set(index);
                    });
                }
                if (seen.cardinality() != records) {
                    throw new AssertionError(
                            "expected "
                                    + records
                                    + " unique records on pass "
                                    + pass
                                    + ", got "
                                    + seen.cardinality());
                }
            }
        }
    }

    private static byte[] payload(int index, int payloadBytes) {
        byte[] value = new byte[payloadBytes];
        ByteBuffer.wrap(value).putInt(index);
        Arrays.fill(value, 4, value.length, (byte) (index & 0x7f));
        return value;
    }

    private static double percentileMs(long[] sorted, double percentile) {
        int index = Math.max(
                0, Math.min(sorted.length - 1, (int) Math.ceil(sorted.length * percentile) - 1));
        return sorted[index] / 1_000_000.0;
    }
}
