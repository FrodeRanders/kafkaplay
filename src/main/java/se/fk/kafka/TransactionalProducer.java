package se.fk.kafka;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class TransactionalProducer {
    protected static final Logger log = LoggerFactory.getLogger(TransactionalProducer.class);

    private KafkaProducer<String, String> producer;

    public TransactionalProducer() {
        // Configure the producer with transactional support
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "unique-transactional-id"); // TODO
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        this.producer = new KafkaProducer<>(props);
        producer.initTransactions();
    }

    public void sendMessagesInTransaction() {
        // Begin the transaction
        String processInstanceId = "...";

        try {
            // Begin the transaction
            producer.beginTransaction();

            // Produce messages
            ProducerRecord<String, String> record1 =
                    new ProducerRecord<>("ProcessA_TaskA_input", processInstanceId, "...");
            producer.send(record1);

            ProducerRecord<String, String> record2 =
                    new ProducerRecord<>("ProcessA_TaskB_input", processInstanceId, "...");
            producer.send(record2);

            // Commit the transaction
            producer.commitTransaction();

        } catch (Exception e) {
            // Rollback the transaction if an error occurs
            producer.abortTransaction();
        }
    }

    // Optionally close the producer
    public void close() {
        producer.close();
    }
}
