package helpers;

import localcluster.KafkaLocalServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.security.Key;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public abstract class KafkaIntegrationTest {

    private static KafkaLocalServer kafkaLocalServer;
    private static final String DEFAULT_KAFKA_LOG_DIR = "/tmp/test/kafka_embedded";
    private static final int BROKER_ID = 0;
    private static final int BROKER_PORT = 5000;
    private static final String LOCALHOST_BROKER = String.format("localhost:%d", BROKER_PORT);

    private static final String DEFAULT_ZOOKEEPER_LOG_DIR = "/tmp/test/zookeeper";
    private static final int ZOOKEEPER_PORT = 2000;
    private static final String ZOOKEEPER_HOST = String.format("localhost:%d", ZOOKEEPER_PORT);

    private static final String groupId = "groupID";

    @BeforeClass
    public static void startKafka() {
        Properties kafkaProperties;
        Properties zkProperties;

        try {
            //load properties
            kafkaProperties = getKafkaProperties(DEFAULT_KAFKA_LOG_DIR, BROKER_PORT, BROKER_ID);
            zkProperties = getZookeeperProperties(ZOOKEEPER_PORT, DEFAULT_ZOOKEEPER_LOG_DIR);

            //start kafkaLocalServer
            kafkaLocalServer = new KafkaLocalServer(kafkaProperties, zkProperties);
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace(System.out);
            Assert.fail("Error running local Kafka broker");
            e.printStackTrace(System.out);
        }
    }

    @AfterClass
    public static void stopKafka() {
        kafkaLocalServer.stop();
    }

    protected static <K,V> List<KeyValue<K, V>> consumeMessagesFromTopic(String topic, String keySerializer, String valueSerializer, int expectedMessageCount, int timeoutInMillis) {
        List<KeyValue<K, V>> result = new ArrayList<>();
        KafkaConsumer consumer = newKafkaConsumer(keySerializer, valueSerializer);

        consumer.subscribe(Arrays.asList(topic));

        long start = System.currentTimeMillis();
        while (result.size() < expectedMessageCount && start - System.currentTimeMillis() < timeoutInMillis) {
            ConsumerRecords<K, V> records = consumer.poll(1000);
            for (ConsumerRecord<K, V> record : records) {
                result.add(new KeyValue<>(record.key(), record.value()));
            }
        }

        consumer.close();

        return result;
    }

    protected static <K, V> void produceMessagesToTopic(
            String topic,
            List<KeyValue<K, V>> messages,
            String keySerializer,
            String valueSerializer) throws ExecutionException, InterruptedException {

        KafkaProducer<K, V> producer = newKafkaProducer(keySerializer, valueSerializer);

        for (KeyValue<K, V> message: messages) {
            ProducerRecord<K, V> data =
                    new ProducerRecord<>(topic, message.getKey(), message.getValue());
            // Produce message synchronously
            producer.send(data).get();
        }

        producer.close();
    }

    private static <K, V> KafkaProducer<K, V> newKafkaProducer(String keySerializer, String valueSerializer) {
        Thread.currentThread().setContextClassLoader(null);

        Properties props = new Properties();
        props.put("bootstrap.servers", LOCALHOST_BROKER);
        props.put("key.serializer", keySerializer);
        props.put("value.serializer", valueSerializer);
        props.put("retries", 3);

        return new KafkaProducer<>(props);
    }

    private static KafkaConsumer newKafkaConsumer(String keySerializer, String valueSerializer) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", LOCALHOST_BROKER);
        properties.put("group.id", groupId);
        properties.put("key.deserializer", keySerializer);
        properties.put("value.deserializer", valueSerializer);
        properties.put("auto.offset.reset", "earliest");

        return new KafkaConsumer(properties);
    }

    private static Properties getKafkaProperties(String logDir, int port, int brokerId) {
        Properties properties = new Properties();
        properties.put("port", port + "");
        properties.put("advertised.listeners", "PLAINTEXT://localhost:5000");
        properties.put("broker.id", brokerId + "");
        properties.put("log.dir", logDir);
        properties.put("zookeeper.connect", ZOOKEEPER_HOST);
        properties.put("delete.topic.enable", "true");
        properties.put("offsets.topic.replication.factor", "1");

        return properties;
    }

    private static Properties getZookeeperProperties(int port, String zookeeperDir) {
        Properties properties = new Properties();
        properties.put("clientPort", port + "");
        properties.put("dataDir", zookeeperDir);
        return properties;
    }
}
