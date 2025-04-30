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
        run1();
        run2();
    }

    private static void run1() {
        try (TransactionalConsumerProducerService consumerProducer = new TransactionalConsumerProducerService()) {
            consumerProducer.processMessageInTransaction();

            // Transaction 1
            try (TransactionalProducer producer = new TransactionalProducer()) {
                producer.sendMessagesInTransaction(1000);
            }

            consumerProducer.processMessageInTransaction();

            // Transaction 2
            try (TransactionalProducer producer = new TransactionalProducer()) {
                producer.sendMessagesInTransaction(1000);
            }

            consumerProducer.processMessageInTransaction();

            // Transaction 3
            try (TransactionalProducer producer = new TransactionalProducer()) {
                producer.sendMessagesInTransaction(1000);
            }

            consumerProducer.processMessageInTransaction();
        }
    }

    private static void run2() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9094");
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
