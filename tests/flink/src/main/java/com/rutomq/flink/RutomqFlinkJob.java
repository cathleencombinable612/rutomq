package com.rutomq.flink;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.KafkaSinkContext;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class RutomqFlinkJob {
    private RutomqFlinkJob() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> options = parseOptions(args);
        String mode = required(options, "mode");
        String bootstrap = required(options, "bootstrap");
        int partitions = Integer.parseInt(options.getOrDefault("partitions", "2"));
        int records = Integer.parseInt(options.getOrDefault("records", "1000"));
        long delayMs = Long.parseLong(options.getOrDefault("delay-ms", "5"));

        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                Duration.ofMillis(500));
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        configureRuntime(env);
        if ("produce".equals(mode)) {
            produce(env, bootstrap, required(options, "topic"), partitions, records, delayMs,
                    required(options, "transactional-prefix"));
        } else if ("transfer".equals(mode)) {
            transfer(
                    env,
                    bootstrap,
                    required(options, "input-topic"),
                    required(options, "output-topic"),
                    required(options, "group-id"),
                    partitions,
                    delayMs,
                    required(options, "transactional-prefix"),
                    options.get("fail-once-marker"));
        } else {
            throw new IllegalArgumentException("unsupported mode " + mode);
        }
        env.execute("rutomq-flink-" + mode);
    }

    private static void configureRuntime(StreamExecutionEnvironment env) {
        env.setParallelism(2);
        env.enableCheckpointing(250, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(Duration.ofSeconds(15).toMillis());
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    }

    private static void produce(
            StreamExecutionEnvironment env,
            String bootstrap,
            String topic,
            int partitions,
            int records,
            long delayMs,
            String transactionalPrefix) {
        DataStream<String> values = env
                .fromSequence(0, records - 1L)
                .setParallelism(1)
                .map(new SequenceMap(delayMs))
                .returns(Types.STRING)
                .name("slow-sequence");
        values
                .rebalance()
                .sinkTo(kafkaSink(bootstrap, topic, partitions, transactionalPrefix))
                .setParallelism(partitions)
                .name("rutomq-exactly-once-sink");
    }

    private static void transfer(
            StreamExecutionEnvironment env,
            String bootstrap,
            String inputTopic,
            String outputTopic,
            String groupId,
            int partitions,
            long delayMs,
            String transactionalPrefix,
            String failOnceMarker) {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setBounded(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("isolation.level", "read_committed")
                .setProperty("enable.auto.commit", "false")
                .setProperty("commit.offsets.on.checkpoint", "true")
                .build();
        DataStream<String> copied = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "rutomq-source")
                .setParallelism(partitions)
                .map(new TransferMap(delayMs, failOnceMarker))
                .returns(Types.STRING)
                .name("checkpointed-transfer");
        copied
                .sinkTo(kafkaSink(bootstrap, outputTopic, partitions, transactionalPrefix))
                .setParallelism(partitions)
                .name("rutomq-transfer-sink");
    }

    private static KafkaSink<String> kafkaSink(
            String bootstrap, String topic, int partitions, String transactionalPrefix) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "600000");
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setKafkaProducerConfig(properties)
                .setRecordSerializer(new PartitioningSchema(topic, partitions))
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(transactionalPrefix)
                .build();
    }

    private static Map<String, String> parseOptions(String[] args) {
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("arguments must be --name value pairs");
        }
        Map<String, String> options = new HashMap<>();
        for (int index = 0; index < args.length; index += 2) {
            String key = args[index];
            if (!key.startsWith("--")) {
                throw new IllegalArgumentException("expected option, got " + key);
            }
            options.put(key.substring(2), args[index + 1]);
        }
        return options;
    }

    private static String required(Map<String, String> options, String key) {
        String value = options.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("--" + key + " is required");
        }
        return value;
    }

    private static final class SequenceMap extends RichMapFunction<Long, String> {
        private final long delayMs;

        private SequenceMap(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public String map(Long value) throws Exception {
            Thread.sleep(delayMs);
            return "flink-" + value;
        }
    }

    private static final class TransferMap extends RichMapFunction<String, String>
            implements CheckpointListener {
        private final long delayMs;
        private final String failOnceMarker;
        private volatile boolean checkpointCompleted;

        private TransferMap(long delayMs, String failOnceMarker) {
            this.delayMs = delayMs;
            this.failOnceMarker = failOnceMarker;
        }

        @Override
        public String map(String value) throws Exception {
            Thread.sleep(delayMs);
            failAfterCheckpointOnce();
            return "copied:" + value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            checkpointCompleted = true;
        }

        private void failAfterCheckpointOnce() throws IOException {
            if (!checkpointCompleted || failOnceMarker == null) {
                return;
            }
            Path marker = Path.of(failOnceMarker);
            try {
                Files.createFile(marker);
                throw new IOException("Injected Flink recovery failure after a completed checkpoint");
            } catch (FileAlreadyExistsException ignored) {
                // A previous attempt already injected the single failure.
            }
        }
    }

    private static final class PartitioningSchema
            implements KafkaRecordSerializationSchema<String> {
        private final String topic;
        private final int partitions;

        private PartitioningSchema(String topic, int partitions) {
            this.topic = topic;
            this.partitions = partitions;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                String element, KafkaSinkContext context, Long timestamp) {
            int separator = element.lastIndexOf('-');
            long sequence = Long.parseLong(element.substring(separator + 1));
            int partition = Math.floorMod(sequence, partitions);
            return new ProducerRecord<>(
                    topic,
                    partition,
                    null,
                    element.getBytes(StandardCharsets.UTF_8));
        }
    }
}
