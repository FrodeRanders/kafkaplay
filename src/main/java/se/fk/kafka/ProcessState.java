package se.fk.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

public record ProcessState(
        String processInstanceId,
        List<Token> tokens,
        Map<String, Object> variables,
        long version
) {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public record Token(String tokenId, String at) {
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize ProcessState", e);
        }
    }

    public static ProcessState fromJson(String json) {
        try {
            return MAPPER.readValue(json, ProcessState.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to parse ProcessState", e);
        }
    }
}
