package se.fk.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ProcessStateStore implements AutoCloseable {
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    public ProcessStateStore(String bootstrapServers, String topic) {
        this.topic = topic;

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(producerProps);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "process-state-reader");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        this.consumer = new KafkaConsumer<>(consumerProps);
    }

    public void save(ProcessState state) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, state.processInstanceId(), state.toJson());
        producer.send(record);
        producer.flush();
    }

    public ProcessState loadLatest(String processInstanceId, Duration timeout) {
        List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                .map(info -> new TopicPartition(topic, info.partition()))
                .toList();

        if (partitions.isEmpty()) {
            return null;
        }

        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        // Naive scan to find the latest value for a key in a compacted topic.
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
        Map<TopicPartition, Long> lastOffsets = new HashMap<>();
        ProcessState latest = null;

        long deadlineMs = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadlineMs) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
            for (var record : records) {
                TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                lastOffsets.put(partition, record.offset());
                if (processInstanceId.equals(record.key())) {
                    latest = ProcessState.fromJson(record.value());
                }
            }

            boolean caughtUp = endOffsets.entrySet().stream().allMatch(entry -> {
                long last = lastOffsets.getOrDefault(entry.getKey(), -1L);
                return last >= entry.getValue() - 1;
            });

            if (caughtUp) {
                return latest;
            }
        }

        return latest;
    }

    @Override
    public void close() {
        producer.close();
        consumer.close();
    }
}
