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
import java.time.Instant;
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

    public long processMessageInTransaction(Duration timeout) {
        consumer.subscribe(Collections.singletonList("ProcessA_TaskB_input"));
        long processed = 0L;

        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {

            // Poll for records.
            // Note that this will only process a batch of messages,
            // say 500 messages, if max.poll.records=500!
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (records.isEmpty()) {
                log.info("No more messages to process");
                continue; // don't break
            }

            log.info("Processing {} records", records.count());
            for (ConsumerRecord<String, String> record : records) {
                try {
                    producer.beginTransaction();

                    String processInstanceId = record.key();
                    String message = record.value();

                    ProducerRecord<String, String> outputRecord =
                            new ProducerRecord<>("ProcessA_TaskB_output",
                                    processInstanceId, message);

                    producer.send(outputRecord);

                    // Commit the consumer offset as part of the transaction.
                    //
                    // When a producer is involved in transactions and commits consumer
                    // offsets with sendOffsetsToTransaction(), then it must know which
                    // group the offsets belong to because it’s acting on behalf of a consumer.
                    // This group metadata is copied from the consumer.
                    producer.sendOffsetsToTransaction(
                            Collections.singletonMap(
                                    new TopicPartition(record.topic(), record.partition()),
                                    new OffsetAndMetadata(record.offset() + 1)
                            ),
                            consumer.groupMetadata()
                    );

                    producer.commitTransaction();
                    processed++;

                } catch (Exception e) {
                    // Abort transaction on failure
                    producer.abortTransaction();
                    log.error("Error sending messages for with transaction {}", transactionalId, e);
                }
            }
        }
        log.info("Processed {} messages with transaction {}", processed, transactionalId);
        return processed;
    }

    public void close() {
        producer.close();
        consumer.close();
    }
}
