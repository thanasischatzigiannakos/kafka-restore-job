package com.example.kafkarestorejob.restoreengine.verification;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class FileReferenceExtractorRegistry {

    private final Map<Class<?>, FileReferenceExtractor<?>> extractorsByPayloadClass;

    public FileReferenceExtractorRegistry(List<FileReferenceExtractor<?>> extractors) {
        Map<Class<?>, FileReferenceExtractor<?>> registry = new LinkedHashMap<>();
        for (FileReferenceExtractor<?> extractor : extractors) {
            FileReferenceExtractor<?> previous = registry.put(extractor.payloadClass(), extractor);
            if (previous != null) {
                throw new IllegalArgumentException(
                        "Duplicate file reference extractor configured for payload class: "
                                + extractor.payloadClass().getName()
                );
            }
        }
        this.extractorsByPayloadClass = Map.copyOf(registry);
    }

    public Collection<FileReference> extract(Class<?> payloadClass, Object payload) {
        FileReferenceExtractor<?> extractor = extractorsByPayloadClass.get(payloadClass);
        if (extractor == null) {
            throw new IllegalArgumentException(
                    "No file reference extractor configured for payload class: "
                            + payloadClass.getName()
            );
        }
        return extractor.extractUntyped(payload);
    }
}
