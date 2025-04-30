package se.fk.kafka.components;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.*;
import io.smallrye.reactive.messaging.MutinyEmitter;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class JoinService {
    protected static final Logger log = LoggerFactory.getLogger(JoinService.class);

    // Non-persistent map to track task completion per process instance
    private final Map<String, Map<String, Boolean>> taskCompletion = new ConcurrentHashMap<>();

    @Inject
    @Channel("processA_taskD_input")
    MutinyEmitter<String> taskDEmitter;

    @Incoming("task-output")
    public CompletionStage<Void> processJoin(Message<String> msg) {
        String payload = msg.getPayload();

        // Extract Kafka headers (works with SmallRye Kafka connector)
        String processInstanceId = msg.getMetadata(Headers.class)
                .map(headers -> {
                    if (headers.lastHeader("processInstanceId") != null) {
                        return new String(headers.lastHeader("processInstanceId").value());
                    } else {
                        return null;
                    }
                })
                .orElse(null);

        String topic = msg.getMetadata(Headers.class)
                .map(this::getTopicFromHeaders)
                .orElse(null);

        if (processInstanceId == null || topic == null) {
            System.err.println("Missing headers. Skipping message: " + payload);
            return msg.ack();
        }

        taskCompletion.computeIfAbsent(processInstanceId, k -> new ConcurrentHashMap<>());

        if (topic.equals("processA_taskB_output")) {
            taskCompletion.get(processInstanceId).put("taskB", true);
        } else if (topic.equals("processA_taskC_output")) {
            taskCompletion.get(processInstanceId).put("taskC", true);
        }

        boolean taskBDone = taskCompletion.get(processInstanceId).getOrDefault("taskB", false);
        boolean taskCDone = taskCompletion.get(processInstanceId).getOrDefault("taskC", false);

        if (taskBDone && taskCDone) {
            taskDEmitter.send("... for " + processInstanceId);
            taskCompletion.remove(processInstanceId);
        }

        return msg.ack();
    }

    private String getTopicFromHeaders(Headers headers) {
        return headers.lastHeader("kafka_receivedTopic") != null
                ? new String(headers.lastHeader("kafka_receivedTopic").value())
                : null;
    }
}
