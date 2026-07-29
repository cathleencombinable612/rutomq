package com.rutomq.flink;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;

public final class ListOffsetsSmoke {
    private ListOffsetsSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("ListOffsetsSmoke <bootstrap> <topic>");
        }
        String bootstrap = args[0];
        String topic = args[1];
        TopicPartition partition = new TopicPartition(topic, 0);

        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            createTopic(admin, topic);
            produceBaseRecords(bootstrap, topic);
            verifyBaseOffsets(admin, partition);
            verifyTransactionIsolation(admin, bootstrap, topic, partition);
        }
        System.out.println(
                "Kafka ListOffsets v11 record timestamps, tier selectors, epochs, and LSO passed");
    }

    private static void verifyBaseOffsets(Admin admin, TopicPartition partition)
            throws Exception {
        assertInfo(
                offset(admin, partition, OffsetSpec.forTimestamp(150), IsolationLevel.READ_UNCOMMITTED),
                1,
                300);
        assertInfo(
                offset(admin, partition, OffsetSpec.maxTimestamp(), IsolationLevel.READ_UNCOMMITTED),
                1,
                300);
        assertInfo(
                offset(admin, partition, OffsetSpec.earliest(), IsolationLevel.READ_UNCOMMITTED),
                0,
                -1);
        assertInfo(
                offset(admin, partition, OffsetSpec.latest(), IsolationLevel.READ_UNCOMMITTED),
                3,
                -1);
        assertInfo(
                offset(admin, partition, OffsetSpec.earliestLocal(), IsolationLevel.READ_UNCOMMITTED),
                0,
                -1);
        assertInfo(
                offset(admin, partition, OffsetSpec.latestTiered(), IsolationLevel.READ_UNCOMMITTED),
                -1,
                -1);
        assertInfo(
                offset(
                        admin,
                        partition,
                        OffsetSpec.earliestPendingUpload(),
                        IsolationLevel.READ_UNCOMMITTED),
                -1,
                -1);
    }

    private static void verifyTransactionIsolation(
            Admin admin, String bootstrap, String topic, TopicPartition partition)
            throws Exception {
        Properties properties = producerProperties(bootstrap);
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "list-offsets-" + topic);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, 0, 400L, "tx", "pending"))
                    .get(15, TimeUnit.SECONDS);

            assertInfo(
                    offset(admin, partition, OffsetSpec.latest(), IsolationLevel.READ_UNCOMMITTED),
                    4,
                    -1);
            assertInfo(
                    offset(admin, partition, OffsetSpec.latest(), IsolationLevel.READ_COMMITTED),
                    3,
                    -1);
            assertInfo(
                    offset(
                            admin,
                            partition,
                            OffsetSpec.forTimestamp(350),
                            IsolationLevel.READ_UNCOMMITTED),
                    3,
                    400);
            assertInfo(
                    offset(
                            admin,
                            partition,
                            OffsetSpec.forTimestamp(350),
                            IsolationLevel.READ_COMMITTED),
                    -1,
                    -1);

            producer.abortTransaction();
            assertInfo(
                    offset(admin, partition, OffsetSpec.latest(), IsolationLevel.READ_COMMITTED),
                    4,
                    -1);
        }
    }

    private static ListOffsetsResultInfo offset(
            Admin admin,
            TopicPartition partition,
            OffsetSpec spec,
            IsolationLevel isolation)
            throws Exception {
        ListOffsetsResultInfo info = admin.listOffsets(
                        Map.of(partition, spec), new ListOffsetsOptions(isolation))
                .partitionResult(partition)
                .get(15, TimeUnit.SECONDS);
        if (info.leaderEpoch().orElse(-1) != 0) {
            throw new AssertionError("unexpected ListOffsets leader epoch " + info);
        }
        return info;
    }

    private static void assertInfo(
            ListOffsetsResultInfo actual, long expectedOffset, long expectedTimestamp) {
        if (actual.offset() != expectedOffset || actual.timestamp() != expectedTimestamp) {
            throw new AssertionError(
                    "ListOffsets mismatch expected=("
                            + expectedOffset
                            + ","
                            + expectedTimestamp
                            + ") actual="
                            + actual);
        }
    }

    private static void produceBaseRecords(String bootstrap, String topic) throws Exception {
        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(producerProperties(bootstrap))) {
            send(producer, topic, 100, "a");
            send(producer, topic, 300, "b");
            send(producer, topic, 200, "c");
        }
    }

    private static void send(
            KafkaProducer<String, String> producer, String topic, long timestamp, String value)
            throws Exception {
        producer.send(new ProducerRecord<>(topic, 0, timestamp, value, value))
                .get(15, TimeUnit.SECONDS);
    }

    private static void createTopic(Admin admin, String topic) throws Exception {
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

    private static Properties adminProperties(String bootstrap) {
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
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000");
        return properties;
    }
}
