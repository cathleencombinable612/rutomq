package com.rutomq.flink;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public final class TopicCompressionSmoke {
    private static final List<String> CODECS =
            List.of("producer", "uncompressed", "gzip", "snappy", "lz4", "zstd");

    private TopicCompressionSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("TopicCompressionSmoke <bootstrap> <topic-prefix>");
        }
        String bootstrap = args[0];
        String prefix = args[1];
        Map<String, byte[]> expected = createTopics(bootstrap, prefix);
        produce(bootstrap, expected);
        consumeAndVerify(bootstrap, expected);
        validateAndResetLevels(bootstrap, prefix);
        System.out.printf(
                "Kafka topic compression policy and levels passed prefix=%s codecs=%s%n",
                prefix, CODECS);
    }

    private static Map<String, byte[]> createTopics(String bootstrap, String prefix)
            throws Exception {
        List<NewTopic> requests = new ArrayList<>();
        Map<String, byte[]> expected = new HashMap<>();
        for (String codec : CODECS) {
            String topic = prefix + "-" + codec;
            Map<String, String> configs = new HashMap<>();
            configs.put("compression.type", codec);
            switch (codec) {
                case "gzip" -> configs.put("compression.gzip.level", "9");
                case "lz4" -> configs.put("compression.lz4.level", "17");
                case "zstd" -> configs.put("compression.zstd.level", "22");
                default -> {
                    // No codec-specific level.
                }
            }
            requests.add(new NewTopic(topic, 1, (short) 1)
                    .configs(configs));
            expected.put(topic, payload(codec));
        }
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(bootstrap))) {
            admin.createTopics(requests).all().get(20, TimeUnit.SECONDS);
            for (String codec : CODECS) {
                assertCompression(admin, prefix + "-" + codec, codec);
            }
            assertConfig(admin, prefix + "-gzip", "compression.gzip.level", "9");
            assertConfig(admin, prefix + "-lz4", "compression.lz4.level", "17");
            assertConfig(admin, prefix + "-zstd", "compression.zstd.level", "22");
        }
        return expected;
    }

    private static byte[] payload(String codec) {
        byte[] label = ("codec=" + codec + ":").getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[16 * 1024];
        for (int index = 0; index < value.length; index++) {
            value[index] = index < label.length ? label[index] : (byte) (index * 31);
        }
        return value;
    }

    private static void produce(String bootstrap, Map<String, byte[]> expected) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
            for (Map.Entry<String, byte[]> entry : expected.entrySet()) {
                producer.send(new ProducerRecord<>(entry.getKey(), 0, null, entry.getValue()))
                        .get(15, TimeUnit.SECONDS);
            }
        }
    }

    private static void consumeAndVerify(String bootstrap, Map<String, byte[]> expected) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            List<TopicPartition> partitions = expected.keySet().stream()
                    .map(topic -> new TopicPartition(topic, 0))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            Map<String, byte[]> actual = new HashMap<>();
            while (actual.size() < expected.size() && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(250))
                        .forEach(record -> actual.put(record.topic(), record.value()));
            }
            for (Map.Entry<String, byte[]> entry : expected.entrySet()) {
                if (!java.util.Arrays.equals(entry.getValue(), actual.get(entry.getKey()))) {
                    throw new AssertionError(
                            "payload mismatch after recompression for " + entry.getKey());
                }
            }
        }
    }

    private static void validateAndResetLevels(String bootstrap, String prefix) throws Exception {
        try (Admin admin = Admin.create(ConsumerProtocolSmoke.adminProperties(bootstrap))) {
            ConfigResource invalid =
                    new ConfigResource(ConfigResource.Type.TOPIC, prefix + "-gzip");
            AlterConfigOp invalidLevel = new AlterConfigOp(
                    new ConfigEntry("compression.gzip.level", "0"),
                    AlterConfigOp.OpType.SET);
            try {
                admin.incrementalAlterConfigs(Map.of(invalid, List.of(invalidLevel)))
                        .all()
                        .get(15, TimeUnit.SECONDS);
                throw new AssertionError("invalid gzip compression level was accepted");
            } catch (ExecutionException expected) {
                // Kafka surfaces the broker's INVALID_REQUEST through this future.
            }

            ConfigResource resource =
                    new ConfigResource(ConfigResource.Type.TOPIC, prefix + "-zstd");
            List<AlterConfigOp> deletes = List.of(
                    delete("compression.type"),
                    delete("compression.gzip.level"),
                    delete("compression.lz4.level"),
                    delete("compression.zstd.level"));
            admin.incrementalAlterConfigs(Map.of(resource, deletes))
                    .all()
                    .get(15, TimeUnit.SECONDS);
            assertCompression(admin, prefix + "-zstd", "producer");
            assertConfig(admin, prefix + "-zstd", "compression.gzip.level", "-1");
            assertConfig(admin, prefix + "-zstd", "compression.lz4.level", "9");
            assertConfig(admin, prefix + "-zstd", "compression.zstd.level", "3");
        }
    }

    private static AlterConfigOp delete(String name) {
        return new AlterConfigOp(new ConfigEntry(name, null), AlterConfigOp.OpType.DELETE);
    }

    private static void assertCompression(Admin admin, String topic, String expected)
            throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Config config = admin.describeConfigs(List.of(resource))
                .all()
                .get(15, TimeUnit.SECONDS)
                .get(resource);
        ConfigEntry entry = config.get("compression.type");
        if (entry == null || !expected.equals(entry.value())) {
            throw new AssertionError(
                    "unexpected compression.type for "
                            + topic
                            + ": "
                            + (entry == null ? null : entry.value()));
        }
    }

    private static void assertConfig(Admin admin, String topic, String name, String expected)
            throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Config config = admin.describeConfigs(List.of(resource))
                .all()
                .get(15, TimeUnit.SECONDS)
                .get(resource);
        ConfigEntry entry = config.get(name);
        if (entry == null || !expected.equals(entry.value())) {
            throw new AssertionError(
                    "unexpected "
                            + name
                            + " for "
                            + topic
                            + ": "
                            + (entry == null ? null : entry.value()));
        }
    }
}
