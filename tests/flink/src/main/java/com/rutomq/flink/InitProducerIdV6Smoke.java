package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class InitProducerIdV6Smoke {
    private static final short INIT_VERSION = 6;
    private static final short END_VERSION = 4;
    private static final int TRANSACTION_TIMEOUT_MS = 3_000;
    private static final String VALUE = "kip-939-prepared";

    private InitProducerIdV6Smoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "InitProducerIdV6Smoke <bootstrap> <topic>");
        }
        String bootstrap = args[0];
        String topic = args[1];
        String transactionalId = "kip-939-" + topic;
        createTopic(bootstrap, topic);

        KafkaProducer<String, String> original =
                new KafkaProducer<>(producerProperties(bootstrap, transactionalId));
        try {
            original.initTransactions();
            original.beginTransaction();
            original.send(new ProducerRecord<>(topic, 0, "prepared", VALUE))
                    .get(15, TimeUnit.SECONDS);
            original.flush();
            assertVisible(bootstrap, topic, false, Duration.ofMillis(750));

            InitProducerIdResponseData recovered =
                    recoverPreparedTransaction(bootstrap, transactionalId);
            Thread.sleep(TRANSACTION_TIMEOUT_MS + 1_500L);
            completePreparedTransaction(bootstrap, transactionalId, recovered);
            assertVisible(bootstrap, topic, true, Duration.ofSeconds(15));
        } finally {
            original.close(Duration.ZERO);
        }

        System.out.printf(
                "Kafka 4.2 InitProducerId v6 KIP-939 recovery passed transactionalId=%s%n",
                transactionalId);
    }

    private static InitProducerIdResponseData recoverPreparedTransaction(
            String bootstrap, String transactionalId) throws Exception {
        InitProducerIdRequestData data =
                new InitProducerIdRequestData()
                        .setTransactionalId(transactionalId)
                        .setTransactionTimeoutMs(TRANSACTION_TIMEOUT_MS)
                        .setProducerId(-1)
                        .setProducerEpoch((short) -1)
                        .setEnable2Pc(true)
                        .setKeepPreparedTxn(true);
        InitProducerIdRequest request =
                new InitProducerIdRequest.Builder(data).build(INIT_VERSION);
        ByteBuffer body = KafkaWire.exchange(bootstrap, request, 93901);
        InitProducerIdResponseData response =
                new InitProducerIdResponseData(
                        new ByteBufferAccessor(body), INIT_VERSION);
        assertNoError("InitProducerId v6", response.errorCode());
        if (response.producerId() < 0
                || response.producerEpoch() < 0
                || response.ongoingTxnProducerId() < 0
                || response.ongoingTxnProducerEpoch() < 0) {
            throw new AssertionError("v6 did not return both producer identities: " + response);
        }
        if (response.producerId() == response.ongoingTxnProducerId()
                && response.producerEpoch()
                        == response.ongoingTxnProducerEpoch()) {
            throw new AssertionError("v6 did not fence the original producer: " + response);
        }
        return response;
    }

    private static void completePreparedTransaction(
            String bootstrap,
            String transactionalId,
            InitProducerIdResponseData recovered)
            throws Exception {
        EndTxnRequestData data =
                new EndTxnRequestData()
                        .setTransactionalId(transactionalId)
                        .setProducerId(recovered.producerId())
                        .setProducerEpoch(recovered.producerEpoch())
                        .setCommitted(true);
        EndTxnRequest request =
                new EndTxnRequest.Builder(data, false).build(END_VERSION);
        ByteBuffer body = KafkaWire.exchange(bootstrap, request, 93902);
        EndTxnResponseData response =
                new EndTxnResponseData(new ByteBufferAccessor(body), END_VERSION);
        assertNoError("EndTxn v4", response.errorCode());
    }

    private static void assertVisible(
            String bootstrap, String topic, boolean expected, Duration timeout) {
        TopicPartition partition = new TopicPartition(topic, 0);
        boolean found = false;
        try (KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(consumerProperties(bootstrap))) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline && !found) {
                for (ConsumerRecord<String, String> record :
                        consumer.poll(Duration.ofMillis(200))) {
                    if (VALUE.equals(record.value())) {
                        found = true;
                        break;
                    }
                }
            }
        }
        if (found != expected) {
            throw new AssertionError(
                    "read_committed visibility expected="
                            + expected
                            + " actual="
                            + found);
        }
    }

    private static void createTopic(String bootstrap, String topic)
            throws Exception {
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

    private static void assertNoError(String operation, short errorCode) {
        if (errorCode != Errors.NONE.code()) {
            throw new AssertionError(
                    operation + " failed with " + Errors.forCode(errorCode));
        }
    }

    private static Properties adminProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        return properties;
    }

    private static Properties producerProperties(
            String bootstrap, String transactionalId) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        properties.put(
                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                Integer.toString(TRANSACTION_TIMEOUT_MS));
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        return properties;
    }

    private static Properties consumerProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kip-939-read-committed-" + System.nanoTime());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        return properties;
    }
}
