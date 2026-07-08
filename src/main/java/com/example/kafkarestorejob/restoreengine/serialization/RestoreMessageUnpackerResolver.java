package com.example.kafkarestorejob.restoreengine.serialization;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class RestoreMessageUnpackerResolver {

    private final Map<String, RestoreMessageUnpacker<?>> unpackersByType = new LinkedHashMap<>();

    public RestoreMessageUnpackerResolver(List<RestoreMessageUnpacker<?>> unpackers) {
        for (RestoreMessageUnpacker<?> unpacker : unpackers) {
            unpackersByType.put(unpacker.messageType(), unpacker);
        }
    }

    public Object unpack(String messageType, byte[] payload) {
        RestoreMessageUnpacker<?> unpacker = unpackersByType.get(messageType);
        if (unpacker == null) {
            throw new IllegalArgumentException("No unpacker configured for message type: " + messageType);
        }
        return unpacker.unpack(payload);
    }
}
