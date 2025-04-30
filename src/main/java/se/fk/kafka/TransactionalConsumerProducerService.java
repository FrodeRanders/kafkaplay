package se.fk.kafka;

import com.fasterxml.uuid.Generators;
import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@ApplicationScoped
public class TransactionalConsumerProducerService implements AutoCloseable {
    protected static final Logger log = LoggerFactory.getLogger(TransactionalConsumerProducerService.class);

    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;
    private final String transactionalId = Generators.timeBasedEpochGenerator().generate().toString();
    private final String groupId = "ProcessA-group-id";

    public TransactionalConsumerProducerService() {
        // Configure the consumer with transaction support
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        consumer = new KafkaConsumer<>(consumerProps);

        // Configure the producer with transactional support
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        producer = new KafkaProducer<>(producerProps);
        producer.initTransactions();
    }

    public void processMessageInTransaction() {
        // Subscribe to the input topic
        consumer.subscribe(Collections.singletonList("ProcessA_TaskB_input"));

        // Poll for records
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        log.info("{} records to process", records.count());
        int count = 0;
        for (ConsumerRecord<String, String> record : records) {
            try {
                // Begin a new transaction
                producer.beginTransaction();

                // Process the consumed message
                String processInstanceId = record.key();
                String message = record.value();

                // Produce results to another topic
                ProducerRecord<String, String> outputRecord =
                        new ProducerRecord<>("ProcessA_TaskB_output",
                                processInstanceId, message);
                producer.send(outputRecord);

                //
                ConsumerGroupMetadata metadata = consumer.groupMetadata();

                // Commit the consumer offset as part of the transaction
                // When a producer is involved in transactions and commits consumer
                // offsets with sendOffsetsToTransaction(), then it must know which
                // group the offsets belong to because it’s acting on behalf of a consumer.
                // This group metadata is copied from the consumer.
                producer.sendOffsetsToTransaction(
                        Collections.singletonMap(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1)
                        ),
                        metadata
                );

                // Commit the transaction
                producer.commitTransaction();
                count++;

            } catch (Exception e) {
                // Abort transaction on failure
                producer.abortTransaction();
                log.error("Error sending messages for with transaction {}", transactionalId, e);
            }
            log.info("Sent {} messages for with transaction {}", count, transactionalId);
        }
    }

    public void close() {
        producer.close();
        consumer.close();
    }
}
