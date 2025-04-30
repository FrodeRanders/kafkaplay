package se.fk.kafka;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.Properties;
import com.fasterxml.uuid.Generators;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class TransactionalProducer implements AutoCloseable {
    protected static final Logger log = LoggerFactory.getLogger(TransactionalProducer.class);

    private KafkaProducer<String, String> producer;
    private final String transactionalId = Generators.timeBasedEpochGenerator().generate().toString();

    public TransactionalProducer() {
        // Configure the producer with transactional support
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        this.producer = new KafkaProducer<>(props);
        producer.initTransactions();
    }

    public void sendMessagesInTransaction(int count) {
        // Begin the transaction
        String processInstanceId = Generators.timeBasedEpochGenerator().generate().toString();

        try {
            producer.beginTransaction();

            for (int i = 0; i < count; i++) {
                // Produce messages
                ProducerRecord<String, String> record1 =
                        new ProducerRecord<>("ProcessA_TaskA_input", processInstanceId, "Process A, Task A, record " + i);
                producer.send(record1);

                ProducerRecord<String, String> record2 =
                        new ProducerRecord<>("ProcessA_TaskB_input", processInstanceId, "Process A, Task B, record " + i);
                producer.send(record2);
            }

            producer.commitTransaction();

            log.info("Sent {} messages for process {} with transaction {}", count, processInstanceId, transactionalId);

        } catch (Exception e) {
            // Rollback the transaction if an error occurs
            producer.abortTransaction();
            log.error("Error sending messages for process {} with transaction {}", processInstanceId, transactionalId, e);
        }
    }

    public void close() {
        producer.close();
    }
}
