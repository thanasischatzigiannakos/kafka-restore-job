package com.example.kafkarestorejob.restoreengine.verification;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class BinaryReferenceExtractorResolver {

    private final Map<String, BinaryReferenceExtractor<?>> extractorsByType = new LinkedHashMap<>();

    public BinaryReferenceExtractorResolver(List<BinaryReferenceExtractor<?>> extractors) {
        for (BinaryReferenceExtractor<?> extractor : extractors) {
            extractorsByType.put(extractor.messageType(), extractor);
        }
    }

    public BinaryReferenceExtractor<?> resolve(String messageType) {
        BinaryReferenceExtractor<?> extractor = extractorsByType.get(messageType);
        if (extractor == null) {
            throw new IllegalArgumentException("No binary reference extractor configured for message type: " + messageType);
        }
        return extractor;
    }
}
