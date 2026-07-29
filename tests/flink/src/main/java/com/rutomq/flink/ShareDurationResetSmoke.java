package com.rutomq.flink;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class ShareDurationResetSmoke {
    private static final int OLD_RECORDS = 2;
    private static final int RECENT_RECORDS = 3;

    private ShareDurationResetSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                    "ShareDurationResetSmoke <bootstrap> <topic> <group>");
        }
        String bootstrap = args[0];
        String topic = args[1];
        String group = args[2];
        createTopic(bootstrap, topic);
        produceTimestampedRecords(bootstrap, topic);
        configureDurationReset(bootstrap, group);

        Set<String> values = consume(bootstrap, topic, group);
        Set<String> expected = Set.of("recent-0", "recent-1", "recent-2");
        if (!values.equals(expected)) {
            throw new AssertionError(
                    "duration reset returned the wrong records expected="
                            + expected
                            + " actual="
                            + values);
        }
        verifyOffset(bootstrap, topic, group);
        System.out.printf(
                "Kafka share by_duration reset passed group=%s startOffset=%d records=%d%n",
                group,
                OLD_RECORDS,
                RECENT_RECORDS);
    }

    private static Set<String> consume(String bootstrap, String topic, String group) {
        Set<String> values = new HashSet<>();
        try (KafkaShareConsumer<String, String> consumer =
                new KafkaShareConsumer<>(consumerProperties(bootstrap, group))) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
            while (System.nanoTime() < deadline && values.size() < RECENT_RECORDS) {
                var records = consumer.poll(Duration.ofMillis(250));
                records.forEach(record -> {
                    if (!record.value().startsWith("recent-")) {
                        throw new AssertionError(
                                "duration reset exposed old record " + record.value());
                    }
                    if (!values.add(record.value())) {
                        throw new AssertionError(
                                "duration reset delivered a duplicate " + record.value());
                    }
                    consumer.acknowledge(record, AcknowledgeType.ACCEPT);
                });
                if (!records.isEmpty()) {
                    consumer.commitSync(Duration.ofSeconds(10));
                }
            }
        }
        return values;
    }

    private static void configureDurationReset(String bootstrap, String group)
            throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.GROUP, group);
        AlterConfigOp reset =
                new AlterConfigOp(
                        new ConfigEntry(
                                "share.auto.offset.reset", "by_duration:PT5M"),
                        AlterConfigOp.OpType.SET);
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            admin.incrementalAlterConfigs(Map.of(resource, List.of(reset)))
                    .all()
                    .get(15, TimeUnit.SECONDS);
            Config config =
                    admin.describeConfigs(List.of(resource))
                            .all()
                            .get(15, TimeUnit.SECONDS)
                            .get(resource);
            ConfigEntry entry = config.get("share.auto.offset.reset");
            if (entry == null
                    || !"by_duration:PT5M".equals(entry.value())
                    || entry.source()
                            != ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG) {
                throw new AssertionError("duration GROUP config did not round-trip: " + entry);
            }
        }
    }

    private static void verifyOffset(String bootstrap, String topic, String group)
            throws Exception {
        TopicPartition partition = new TopicPartition(topic, 0);
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            Map<TopicPartition, SharePartitionOffsetInfo> offsets =
                    admin.listShareGroupOffsets(
                                    Map.of(
                                            group,
                                            new ListShareGroupOffsetsSpec()
                                                    .topicPartitions(
                                                            List.of(partition))))
                            .partitionsToOffsetInfo(group)
                            .get(15, TimeUnit.SECONDS);
            SharePartitionOffsetInfo info = offsets.get(partition);
            if (info == null
                    || info.startOffset() != OLD_RECORDS + RECENT_RECORDS
                    || info.lag().orElse(-1L) != 0) {
                throw new AssertionError("unexpected duration-reset share offset " + info);
            }
        }
    }

    private static void createTopic(String bootstrap, String topic) throws Exception {
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
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
    }

    private static void produceTimestampedRecords(String bootstrap, String topic)
            throws Exception {
        long now = System.currentTimeMillis();
        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(producerProperties(bootstrap))) {
            for (int index = 0; index < OLD_RECORDS; index++) {
                producer.send(
                        new ProducerRecord<>(
                                topic,
                                0,
                                now - Duration.ofHours(1).toMillis(),
                                "old-" + index,
                                "old-" + index));
            }
            for (int index = 0; index < RECENT_RECORDS; index++) {
                producer.send(
                        new ProducerRecord<>(
                                topic,
                                0,
                                now - Duration.ofSeconds(30).toMillis(),
                                "recent-" + index,
                                "recent-" + index));
            }
            producer.flush();
        }
    }

    private static Properties consumerProperties(String bootstrap, String group) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");
        properties.put(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "record_limit");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        return properties;
    }

    private static Properties producerProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        return properties;
    }

    private static Properties adminProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        return properties;
    }
}
