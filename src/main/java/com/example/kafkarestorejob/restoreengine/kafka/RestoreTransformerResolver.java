package com.example.kafkarestorejob.restoreengine.kafka;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class RestoreTransformerResolver {

    private final Map<String, RestoreTransformer> transformersByType;

    public RestoreTransformerResolver(List<RestoreTransformer> transformers) {
        this.transformersByType = new LinkedHashMap<>();
        for (RestoreTransformer transformer : transformers) {
            transformersByType.put(transformer.messageType(), transformer);
        }
    }

    public RestoreTransformer resolve(String messageType) {
        RestoreTransformer transformer = transformersByType.get(messageType);
        if (transformer == null) {
            throw new IllegalArgumentException("No transformer configured for message type: " + messageType);
        }
        return transformer;
    }
}
