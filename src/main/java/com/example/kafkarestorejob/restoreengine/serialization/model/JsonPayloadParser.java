package com.example.kafkarestorejob.restoreengine.serialization.model;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Small JSON parser utility backing the placeholder message models in this repository.
 */
final class JsonPayloadParser {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Prevents instantiation of the static utility.
     */
    private JsonPayloadParser() {
    }

    /**
     * Parses the supplied JSON bytes into the requested payload type.
     *
     * @param payloadBytes the serialized payload bytes
     * @param payloadClass the target payload class
     * @param <T> the payload type
     * @return the parsed payload
     */
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
