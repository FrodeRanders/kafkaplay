package se.fk.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public record ProcessEvent(
        String processInstanceId,
        String processId,
        String activityId,
        String tokenId,
        String correlationId,
        Map<String, Object> payload,
        Status status,
        ErrorInfo error
) {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public enum Status {
        STARTED,
        COMPLETED,
        FAILED
    }

    public record ErrorInfo(String message, String code) {
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize ProcessEvent", e);
        }
    }

    public static ProcessEvent fromJson(String json) {
        try {
            return MAPPER.readValue(json, ProcessEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to parse ProcessEvent", e);
        }
    }
}
