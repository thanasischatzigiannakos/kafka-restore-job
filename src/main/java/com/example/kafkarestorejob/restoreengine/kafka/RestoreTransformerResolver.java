package com.example.kafkarestorejob.restoreengine.kafka;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class RestoreTransformerResolver {

    private final Map<String, RestoreTransformer> transformersByType;

    public RestoreTransformerResolver(List<RestoreTransformer> transformers) {
        Map<String, RestoreTransformer> registry = new LinkedHashMap<>();
        for (RestoreTransformer transformer : transformers) {
            RestoreTransformer previous = registry.put(transformer.messageType(), transformer);
            if (previous != null) {
                throw new IllegalArgumentException("Duplicate transformer configured for message type: " + transformer.messageType());
            }
        }
        this.transformersByType = Map.copyOf(registry);
    }

    public RestoreTransformer resolve(String messageType) {
        RestoreTransformer transformer = transformersByType.get(messageType);
        if (transformer == null) {
            throw new IllegalArgumentException("No transformer configured for message type: " + messageType);
        }
        return transformer;
    }
}
