package se.fk.kafka.components;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class XorGatewayService {
    protected static final Logger log = LoggerFactory.getLogger(XorGatewayService.class);

    @Inject
    @Channel("processA_taskB_input")
    Emitter<String> taskBEmitter;

    @Inject
    @Channel("processA_taskC_input")
    Emitter<String> taskCEmitter;

    @Incoming("processA_taskA_output")
    public CompletionStage<Void> processGateway(Message<String> msg) {
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

        if (processInstanceId == null) {
            System.err.println("Missing processInstanceId in message metadata.");
            return msg.ack();
        }

        // ⛳️ Replace this with real business logic
        boolean condition = evaluateCondition(payload);

        if (condition) {
            taskBEmitter.send(payload);
        } else {
            taskCEmitter.send(payload);
        }

        return msg.ack();
    }

    private boolean evaluateCondition(String payload) {
        // Implement your actual XOR logic here
        return payload.hashCode() % 2 == 0;
    }
}
