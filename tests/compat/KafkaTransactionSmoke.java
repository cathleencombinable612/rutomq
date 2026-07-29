import java.time.Duration;
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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class KafkaTransactionSmoke {
    private KafkaTransactionSmoke() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: KafkaTransactionSmoke <bootstrap> <topic>");
        }
        String bootstrap = args[0];
        String topic = args[1];
        String group = topic + "-group";
        String transactionalId = topic + "-tx";
        TopicPartition partition = new TopicPartition(topic, 0);
        Properties adminProperties = new Properties();
        adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        adminProperties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        ConfigResource topicResource =
                new ConfigResource(ConfigResource.Type.TOPIC, topic);

        try (Admin admin = Admin.create(adminProperties)) {
            try {
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                        .all()
                        .get(10, TimeUnit.SECONDS);
            } catch (ExecutionException error) {
                if (!(error.getCause() instanceof TopicExistsException)) {
                    throw error;
                }
            }
            AlterConfigOp retention = new AlterConfigOp(
                    new ConfigEntry("retention.ms", "600000"),
                    AlterConfigOp.OpType.SET);
            admin.incrementalAlterConfigs(Map.of(topicResource, List.of(retention)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            Config config = admin.describeConfigs(List.of(topicResource))
                    .all()
                    .get(10, TimeUnit.SECONDS)
                    .get(topicResource);
            if (!"600000".equals(config.get("retention.ms").value())) {
                throw new AssertionError("topic retention config did not round-trip");
            }
        }

        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        producerProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        producerProperties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, 0, "key", "committed"))
                    .get(10, TimeUnit.SECONDS);
            producer.sendOffsetsToTransaction(
                    Map.of(partition, new OffsetAndMetadata(1)),
                    new ConsumerGroupMetadata(group));
            producer.commitTransaction();

            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, 0, "key", "aborted"))
                    .get(10, TimeUnit.SECONDS);
            producer.abortTransaction();
        }

        try (Admin admin = Admin.create(adminProperties)) {
            boolean listed = admin.listTransactions()
                    .all()
                    .get(10, TimeUnit.SECONDS)
                    .stream()
                    .anyMatch(transaction -> transactionalId.equals(transaction.transactionalId()));
            if (!listed) {
                throw new AssertionError("transactional id was not listed");
            }
            var described = admin.describeTransactions(List.of(transactionalId))
                    .all()
                    .get(10, TimeUnit.SECONDS)
                    .get(transactionalId);
            if (described == null || described.producerId() < 0) {
                throw new AssertionError("transactional id was not described");
            }
        }

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put("group.protocol", "classic");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            boolean committedSeen = false;
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(250))) {
                    if ("aborted".equals(record.value())) {
                        throw new AssertionError("read_committed exposed an aborted record");
                    }
                    if ("committed".equals(record.value())) {
                        committedSeen = true;
                    }
                }
                if (committedSeen) {
                    break;
                }
            }
            if (!committedSeen) {
                throw new AssertionError("committed transaction was not fetched");
            }
            OffsetAndMetadata committedOffset = consumer.committed(Set.of(partition)).get(partition);
            if (committedOffset == null || committedOffset.offset() != 1) {
                throw new AssertionError("transactional offset was not committed");
            }
        }

        System.out.println("Kafka 4.0 transaction and config smoke passed");
    }
}
