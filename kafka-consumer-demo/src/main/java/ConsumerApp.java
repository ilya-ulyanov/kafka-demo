import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerApp.class);
    private static final String KAFKA_SERVER = "localhost:9092";
    private static final String TOPIC_NAME = "coding-demo-01";
    private static final String CONSUMER_GROUP = "consumer-app-01";

    public static void main(String[] args) {
        // 1. Create KafkaConsumer properties
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);

        // 2. Create KafkaConsumer instance
        var consumer = new KafkaConsumer<String, String>(properties);

        // 3. Subscribe to a topic
        consumer.subscribe(Collections.singleton(TOPIC_NAME));

        // 4. Polling for records and print them out
        while (true) {
            LOGGER.info("Polling...");
            var records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                for (var record : records) {
                    LOGGER.info(record.toString());
                }
            }
        }
    }
}
