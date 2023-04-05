import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerApp.class);
    private static final String KAFKA_SERVER = "localhost:9092";
    private static final String TOPIC_NAME = "coding-demo-01";

    public static void main(String[] args) {
        // 1. Create KafkaProducer properties
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 2. Create KafkaProducer instance
        var producer = new KafkaProducer<String, String>(properties);

        // 3. Create a Record
        var record = new ProducerRecord<String, String>(TOPIC_NAME, "hello world!");

        // 4. Send the record - async operation!
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                LOGGER.info("""
                    Record was successfully sent, metadata:
                     - Topic: {}
                     - Partition: {}
                     - Offset: {}
                     - Timestamp: {}""", metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
            } else {
                LOGGER.error("Error occurred while sending a record to Kafka", exception);
            }
        });

        // 5. Flush the KafkaProducer's buffer - sync operation
        producer.flush();

        // 6. Close the KafkaProducer - flushes the buffer too
        producer.close();
    }
}
