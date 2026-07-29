package com.rutomq.flink;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterClientQuotasOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class ClientQuotaSmoke {
    private ClientQuotaSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("ClientQuotaSmoke <bootstrap> <topic>");
        }
        String bootstrap = args[0];
        String topic = args[1];
        ClientQuotaEntity defaultClient = entity("ANONYMOUS", null);
        try (Admin admin = Admin.create(adminProperties(bootstrap))) {
            verifyValidateOnly(admin, defaultClient);
            set(admin, defaultClient, List.of(
                    op("producer_byte_rate", 1_000_000.0),
                    op("consumer_byte_rate", 1_000_000.0),
                    op("request_percentage", 10_000.0),
                    op("controller_mutation_rate", 10_000.0)));
            verifyFilters(admin, defaultClient);
            verifyInvalidIntegralQuota(admin);

            ClientQuotaEntity ip = new ClientQuotaEntity(
                    Map.of(ClientQuotaEntity.IP, "127.0.0.1"));
            set(admin, ip, List.of(op("connection_creation_rate", 1_000.0)));
            Map<ClientQuotaEntity, Map<String, Double>> ipResult = admin.describeClientQuotas(
                            ClientQuotaFilter.containsOnly(List.of(
                                    ClientQuotaFilterComponent.ofEntity(
                                            ClientQuotaEntity.IP, "127.0.0.1"))))
                    .entities()
                    .get(10, TimeUnit.SECONDS);
            if (ipResult.getOrDefault(ip, Map.of()).get("connection_creation_rate") != 1_000.0) {
                throw new AssertionError("IP connection quota did not round-trip " + ipResult);
            }
            remove(admin, ip, "connection_creation_rate");

            ClientQuotaEntity throttled = entity("ANONYMOUS", "quota-producer");
            set(admin, throttled, List.of(op("producer_byte_rate", 2_048.0)));
            createTopic(admin, topic);
            verifyProduceThrottle(bootstrap, topic);
            remove(admin, throttled, "producer_byte_rate");

            ClientQuotaEntity throttledConsumer = entity("ANONYMOUS", "quota-consumer");
            set(admin, throttledConsumer, List.of(op("consumer_byte_rate", 2_048.0)));
            verifyFetchThrottle(bootstrap, topic);
            remove(admin, throttledConsumer, "consumer_byte_rate");

            remove(
                    admin,
                    defaultClient,
                    "producer_byte_rate",
                    "consumer_byte_rate",
                    "request_percentage",
                    "controller_mutation_rate");
            Map<ClientQuotaEntity, Map<String, Double>> remaining = admin.describeClientQuotas(
                            ClientQuotaFilter.all())
                    .entities()
                    .get(10, TimeUnit.SECONDS);
            if (remaining.containsKey(defaultClient)
                    || remaining.containsKey(ip)
                    || remaining.containsKey(throttled)
                    || remaining.containsKey(throttledConsumer)) {
                throw new AssertionError("removed client quotas remained visible " + remaining);
            }
        }
        System.out.println("Kafka Admin client quota CRUD and runtime throttling passed");
    }

    private static void verifyValidateOnly(Admin admin, ClientQuotaEntity entity)
            throws Exception {
        ClientQuotaAlteration alteration = new ClientQuotaAlteration(
                entity, List.of(op("producer_byte_rate", 123_456.0)));
        admin.alterClientQuotas(
                        List.of(alteration), new AlterClientQuotasOptions().validateOnly(true))
                .all()
                .get(10, TimeUnit.SECONDS);
        if (admin.describeClientQuotas(ClientQuotaFilter.all())
                .entities()
                .get(10, TimeUnit.SECONDS)
                .containsKey(entity)) {
            throw new AssertionError("validate-only client quota mutated metadata");
        }
    }

    private static void verifyFilters(Admin admin, ClientQuotaEntity entity) throws Exception {
        List<ClientQuotaFilterComponent> exact = List.of(
                ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "ANONYMOUS"),
                ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.CLIENT_ID));
        Map<ClientQuotaEntity, Map<String, Double>> strict = admin.describeClientQuotas(
                        ClientQuotaFilter.containsOnly(exact))
                .entities()
                .get(10, TimeUnit.SECONDS);
        if (strict.size() != 1
                || strict.getOrDefault(entity, Map.of()).get("producer_byte_rate")
                        != 1_000_000.0) {
            throw new AssertionError("strict client quota filter mismatch " + strict);
        }
        Map<ClientQuotaEntity, Map<String, Double>> contains = admin.describeClientQuotas(
                        ClientQuotaFilter.contains(List.of(
                                ClientQuotaFilterComponent.ofEntity(
                                        ClientQuotaEntity.USER, "ANONYMOUS"))))
                .entities()
                .get(10, TimeUnit.SECONDS);
        if (!contains.containsKey(entity)) {
            throw new AssertionError("non-strict client quota filter missed entity " + contains);
        }
        Map<ClientQuotaEntity, Map<String, Double>> tooStrict = admin.describeClientQuotas(
                        ClientQuotaFilter.containsOnly(List.of(
                                ClientQuotaFilterComponent.ofEntity(
                                        ClientQuotaEntity.USER, "ANONYMOUS"))))
                .entities()
                .get(10, TimeUnit.SECONDS);
        if (!tooStrict.isEmpty()) {
            throw new AssertionError("strict client quota filter included extra dimensions");
        }
    }

    private static void verifyInvalidIntegralQuota(Admin admin) throws Exception {
        ClientQuotaEntity entity = new ClientQuotaEntity(
                Map.of(ClientQuotaEntity.CLIENT_ID, "invalid-integral"));
        try {
            set(admin, entity, List.of(op("producer_byte_rate", 1.5)));
            throw new AssertionError("fractional producer byte quota was accepted");
        } catch (ExecutionException error) {
            if (!(error.getCause() instanceof InvalidRequestException)) {
                throw error;
            }
        }
    }

    private static void verifyProduceThrottle(String bootstrap, String topic) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "quota-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000");
        String value = "x".repeat(4_096);
        long started = System.nanoTime();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            producer.send(new ProducerRecord<>(topic, 0, "quota", value))
                    .get(12, TimeUnit.SECONDS);
        }
        Duration elapsed = Duration.ofNanos(System.nanoTime() - started);
        if (elapsed.compareTo(Duration.ofMillis(500)) < 0) {
            throw new AssertionError("produce quota did not throttle request: " + elapsed);
        }
    }

    private static void verifyFetchThrottle(String bootstrap, String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "quota-consumer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "quota-consumer-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        TopicPartition partition = new TopicPartition(topic, 0);
        long started = System.nanoTime();
        boolean found = false;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.nanoTime() + Duration.ofSeconds(12).toNanos();
            while (System.nanoTime() < deadline && !found) {
                found = consumer.poll(Duration.ofMillis(500)).records(partition).stream()
                        .anyMatch(record -> record.value().length() == 4_096);
            }
        }
        Duration elapsed = Duration.ofNanos(System.nanoTime() - started);
        if (!found || elapsed.compareTo(Duration.ofMillis(500)) < 0) {
            throw new AssertionError(
                    "fetch quota did not throttle and recover: found=" + found
                            + " elapsed=" + elapsed);
        }
    }

    private static void createTopic(Admin admin, String topic) throws Exception {
        try {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
        } catch (ExecutionException error) {
            if (!(error.getCause() instanceof TopicExistsException)) {
                throw error;
            }
        }
    }

    private static void set(
            Admin admin, ClientQuotaEntity entity, List<ClientQuotaAlteration.Op> ops)
            throws Exception {
        admin.alterClientQuotas(List.of(new ClientQuotaAlteration(entity, ops)))
                .all()
                .get(10, TimeUnit.SECONDS);
    }

    private static void remove(Admin admin, ClientQuotaEntity entity, String... keys)
            throws Exception {
        set(
                admin,
                entity,
                java.util.Arrays.stream(keys).map(key -> op(key, null)).toList());
    }

    private static ClientQuotaAlteration.Op op(String key, Double value) {
        return new ClientQuotaAlteration.Op(key, value);
    }

    private static ClientQuotaEntity entity(String user, String clientId) {
        Map<String, String> entries = new HashMap<>();
        entries.put(ClientQuotaEntity.USER, user);
        entries.put(ClientQuotaEntity.CLIENT_ID, clientId);
        return new ClientQuotaEntity(entries);
    }

    private static Properties adminProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        return properties;
    }
}
