package se.fk.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class App {
    private static final Logger log = LogManager.getLogger(App.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo", "Hello world");

            kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
                if (e != null) {
                    return;
                }

                log.info("Topic {}", recordMetadata.topic());
                log.info("Offset {}", recordMetadata.offset());
                log.info("Partition {}", recordMetadata.partition());
                log.info("Timestamp {}", recordMetadata.timestamp());
            });
            kafkaProducer.flush();
        }
    }
}
