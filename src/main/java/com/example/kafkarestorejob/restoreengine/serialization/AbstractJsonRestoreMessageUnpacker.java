package com.example.kafkarestorejob.restoreengine.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractJsonRestoreMessageUnpacker<T> implements RestoreMessageUnpacker<T> {

    private final ObjectMapper objectMapper;

    protected AbstractJsonRestoreMessageUnpacker(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public T unpack(byte[] payload) {
        try {
            return objectMapper.readValue(payload, payloadClass());
        } catch (Exception exception) {
            throw new MessageUnpackingException("Failed to unpack " + messageType() + " payload", exception);
        }
    }
}
