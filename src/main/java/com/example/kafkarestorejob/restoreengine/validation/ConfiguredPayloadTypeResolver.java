package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.serialization.RestoreMessageUnpacker;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class ConfiguredPayloadTypeResolver implements ExpectedPayloadTypeResolver {

    private final Map<String, Class<?>> payloadClassesByMessageType;

    public ConfiguredPayloadTypeResolver(List<RestoreMessageUnpacker<?>> unpackers) {
        Map<String, Class<?>> registry = new LinkedHashMap<>();
        for (RestoreMessageUnpacker<?> unpacker : unpackers) {
            Class<?> previous = registry.put(unpacker.messageType(), unpacker.payloadClass());
            if (previous != null) {
                throw new IllegalArgumentException(
                        "Duplicate payload type configured for message type: " + unpacker.messageType()
                );
            }
        }
        this.payloadClassesByMessageType = Map.copyOf(registry);
    }

    @Override
    public Class<?> resolve(String configuredMessageType) {
        Class<?> payloadClass = payloadClassesByMessageType.get(configuredMessageType);
        if (payloadClass == null) {
            throw new IllegalArgumentException(
                    "No payload type configured for message type: " + configuredMessageType
            );
        }
        return payloadClass;
    }
}
