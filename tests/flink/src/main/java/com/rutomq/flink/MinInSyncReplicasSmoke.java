package com.rutomq.flink;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.serialization.StringSerializer;

public final class MinInSyncReplicasSmoke {
    private static final short PRODUCE_VERSION = 3;

    private MinInSyncReplicasSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "MinInSyncReplicasSmoke <bootstrap> <topic>");
        }
        String bootstrap = args[0];
        String topic = args[1];
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);

        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            admin.createTopics(List.of(
                            new NewTopic(topic, 1, (short) 1)
                                    .configs(Map.of("min.insync.replicas", "2"))))
                    .all()
                    .get(15, TimeUnit.SECONDS);
            assertConfig(admin, resource, "2");
            expectConfigFailure(admin, resource);
        }

        expectAllAcksFailure(bootstrap, topic);
        assertInvalidRequiredAcks(bootstrap, topic);
        RecordMetadata singleReplica = send(bootstrap, topic, "1", "acks-one");
        if (singleReplica.offset() != 0) {
            throw new AssertionError(
                    "rejected Produce allocated an offset: " + singleReplica.offset());
        }

        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            alter(
                    admin,
                    resource,
                    List.of(new AlterConfigOp(
                            new ConfigEntry("min.insync.replicas", null),
                            AlterConfigOp.OpType.DELETE)));
            assertConfig(admin, resource, "1");
        }
        RecordMetadata recovered = send(bootstrap, topic, "all", "acks-all");
        if (recovered.offset() != 1) {
            throw new AssertionError(
                    "acks=all did not recover after config DELETE: " + recovered.offset());
        }

        System.out.printf(
                "Kafka min.insync.replicas passed topic=%s virtualIsr=1 offsets=0..1%n",
                topic);
    }

    private static RecordMetadata send(
            String bootstrap, String topic, String acks, String value) throws Exception {
        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(producerProperties(bootstrap, acks))) {
            return producer.send(new ProducerRecord<>(topic, 0, "key", value))
                    .get(15, TimeUnit.SECONDS);
        }
    }

    private static void expectAllAcksFailure(String bootstrap, String topic)
            throws Exception {
        try {
            send(bootstrap, topic, "all", "must-fail");
            throw new AssertionError("acks=all ignored min.insync.replicas=2");
        } catch (ExecutionException expected) {
            if (!(expected.getCause() instanceof NotEnoughReplicasException)) {
                throw expected;
            }
        }
    }

    private static void assertInvalidRequiredAcks(String bootstrap, String topic)
            throws Exception {
        ProduceRequestData.PartitionProduceData partition =
                new ProduceRequestData.PartitionProduceData().setIndex(0);
        ProduceRequestData.TopicProduceData topicData =
                new ProduceRequestData.TopicProduceData()
                        .setName(topic)
                        .setPartitionData(List.of(partition));
        ProduceRequestData data =
                new ProduceRequestData()
                        .setAcks((short) 2)
                        .setTimeoutMs(1_000)
                        .setTopicData(
                                new ProduceRequestData.TopicProduceDataCollection(
                                        List.of(topicData).iterator()));
        ProduceRequest request =
                new ProduceRequest.Builder(PRODUCE_VERSION, PRODUCE_VERSION, data)
                        .build(PRODUCE_VERSION);
        ByteBuffer body = KafkaWire.exchange(bootstrap, request, 41001);
        ProduceResponseData response =
                new ProduceResponseData(
                        new ByteBufferAccessor(body), PRODUCE_VERSION);
        short errorCode =
                response.responses().iterator().next().partitionResponses().get(0).errorCode();
        if (errorCode != Errors.INVALID_REQUIRED_ACKS.code()) {
            throw new AssertionError(
                    "invalid required acks returned " + Errors.forCode(errorCode));
        }
    }

    private static void expectConfigFailure(Admin admin, ConfigResource resource)
            throws Exception {
        try {
            alter(
                    admin,
                    resource,
                    List.of(new AlterConfigOp(
                            new ConfigEntry("min.insync.replicas", "0"),
                            AlterConfigOp.OpType.SET)));
            throw new AssertionError("min.insync.replicas=0 was accepted");
        } catch (ExecutionException expected) {
            // Kafka exposes the broker's per-resource configuration error.
        }
    }

    private static void alter(
            Admin admin, ConfigResource resource, List<AlterConfigOp> changes) throws Exception {
        admin.incrementalAlterConfigs(Map.of(resource, changes))
                .all()
                .get(15, TimeUnit.SECONDS);
    }

    private static void assertConfig(
            Admin admin, ConfigResource resource, String expected) throws Exception {
        Config config = admin.describeConfigs(List.of(resource))
                .all()
                .get(15, TimeUnit.SECONDS)
                .get(resource);
        ConfigEntry entry = config.get("min.insync.replicas");
        if (entry == null || !expected.equals(entry.value())) {
            throw new AssertionError(
                    "unexpected min.insync.replicas expected="
                            + expected
                            + " actual="
                            + entry);
        }
    }

    private static Properties adminProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        return properties;
    }

    private static Properties producerProperties(String bootstrap, String acks) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "minimum-isr-" + acks);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, acks);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000");
        return properties;
    }
}
