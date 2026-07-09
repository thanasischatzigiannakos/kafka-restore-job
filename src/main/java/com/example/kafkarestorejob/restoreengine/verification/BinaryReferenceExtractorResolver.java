package com.example.kafkarestorejob.restoreengine.verification;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class BinaryReferenceExtractorResolver {

    private final Map<String, BinaryReferenceExtractor<?>> extractorsByType;

    public BinaryReferenceExtractorResolver(List<BinaryReferenceExtractor<?>> extractors) {
        Map<String, BinaryReferenceExtractor<?>> registry = new LinkedHashMap<>();
        for (BinaryReferenceExtractor<?> extractor : extractors) {
            BinaryReferenceExtractor<?> previous = registry.put(extractor.messageType(), extractor);
            if (previous != null) {
                throw new IllegalArgumentException("Duplicate binary reference extractor configured for message type: " + extractor.messageType());
            }
        }
        this.extractorsByType = Map.copyOf(registry);
    }

    public BinaryReferenceExtractor<?> resolve(String messageType) {
        BinaryReferenceExtractor<?> extractor = extractorsByType.get(messageType);
        if (extractor == null) {
            throw new IllegalArgumentException("No binary reference extractor configured for message type: " + messageType);
        }
        return extractor;
    }
}
