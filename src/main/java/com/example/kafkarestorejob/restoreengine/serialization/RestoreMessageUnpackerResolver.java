package com.example.kafkarestorejob.restoreengine.serialization;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class RestoreMessageUnpackerResolver {

    private final Map<String, RestoreMessageUnpacker<?>> unpackersByType;

    public RestoreMessageUnpackerResolver(List<RestoreMessageUnpacker<?>> unpackers) {
        Map<String, RestoreMessageUnpacker<?>> registry = new LinkedHashMap<>();
        for (RestoreMessageUnpacker<?> unpacker : unpackers) {
            RestoreMessageUnpacker<?> previous = registry.put(unpacker.messageType(), unpacker);
            if (previous != null) {
                throw new IllegalArgumentException("Duplicate unpacker configured for message type: " + unpacker.messageType());
            }
        }
        this.unpackersByType = Map.copyOf(registry);
    }

    public Object unpack(String messageType, byte[] payload) {
        RestoreMessageUnpacker<?> unpacker = resolve(messageType);
        return unpacker.unpack(payload);
    }

    public RestoreMessageUnpacker<?> resolve(String messageType) {
        RestoreMessageUnpacker<?> unpacker = unpackersByType.get(messageType);
        if (unpacker == null) {
            throw new IllegalArgumentException("No unpacker configured for message type: " + messageType);
        }
        return unpacker;
    }
}
