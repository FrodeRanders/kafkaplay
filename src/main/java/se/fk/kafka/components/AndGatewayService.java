package se.fk.kafka.components;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class AndGatewayService {

    @Inject
    @Channel("processA_taskB_input")
    Emitter<String> taskBEmitter;

    @Inject
    @Channel("processA_taskC_input")
    Emitter<String> taskCEmitter;

    @Incoming("processA_taskA_output")
    public CompletionStage<Void> processAndGateway(Message<String> msg) {
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
            System.err.println("Missing processInstanceId header.");
            return msg.ack();
        }

        // Fan-out to both Task B and Task C
        taskBEmitter.send(payload);
        taskCEmitter.send(payload);

        return msg.ack();
    }
}
