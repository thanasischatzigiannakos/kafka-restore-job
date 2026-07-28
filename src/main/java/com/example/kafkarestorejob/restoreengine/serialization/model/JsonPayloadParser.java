package com.example.kafkarestorejob.restoreengine.serialization.model;

import com.fasterxml.jackson.databind.ObjectMapper;

final class JsonPayloadParser {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private JsonPayloadParser() {
    }

    static <T> T parse(byte[] payloadBytes, Class<T> payloadClass) {
        try {
            return OBJECT_MAPPER.readValue(payloadBytes, payloadClass);
        } catch (Exception exception) {
            throw new IllegalArgumentException(
                    "Failed to parse payload for " + payloadClass.getSimpleName(),
                    exception
            );
        }
    }
}
