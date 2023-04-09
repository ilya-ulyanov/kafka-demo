import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerApp.class);
    private static final String KAFKA_SERVER = "localhost:9092";
    private static final String TOPIC_NAME = "coding-demo-01";
    private static final String CONSUMER_GROUP = "consumer-app-01";
    private final KafkaConsumer<String, String> consumer;
    private final CountDownLatch sync = new CountDownLatch(1);

    public ConsumerApp() {
        // 1. Create KafkaConsumer properties
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        // 2. Create KafkaConsumer instance
        consumer = new KafkaConsumer<>(properties);
    }

    public void start() {
        // 3. Subscribe to a topic
        consumer.subscribe(Collections.singleton(TOPIC_NAME));

        // 4. Polling for records and print them out
        pollAndPrint();
    }

    private void pollAndPrint() {
        try {
            while (true) {
                var records = consumer.poll(Duration.ofSeconds(1));
                if (!records.isEmpty()) {
                    for (var record : records) {
                        LOGGER.info(record.toString());
                    }
                }
            }
        } catch (WakeupException e) {
            LOGGER.info("KafkaConsumer was waked up and going to be closed...");
        } catch (Exception e) {
            LOGGER.error("Unexpected error occurred", e);
        } finally {
            consumer.close();
            LOGGER.info("KafkaConsumer was closed");
            sync.countDown();
        }
    }

    public void stop() {
        consumer.wakeup();
        try {
            sync.await();
        } catch (InterruptedException e) {
            // nothing
        }
    }

    public static void main(String[] args) {
        var consumerApp = new ConsumerApp();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutdown was requested. Stopping ConsumerApp...");
            consumerApp.stop();
            LOGGER.info("ConsumerApp was gracefully stopped");
        }));

        consumerApp.start();
    }
}
