package com.example.kafkarestorejob.restoreengine.verification;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class FileReferenceExtractorRegistry {

    private final Map<Class<?>, FileReferenceExtractor<?>> extractorsByClass;

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
        this.extractorsByClass = Map.copyOf(registry);
    }

    public boolean supports(Class<?> payloadClass) {
        return extractorsByClass.containsKey(payloadClass);
    }

    public FileReferenceExtractor<?> requireExtractor(Class<?> payloadClass) {
        FileReferenceExtractor<?> extractor = extractorsByClass.get(payloadClass);
        if (extractor == null) {
            throw new IllegalArgumentException(
                    "No file reference extractor configured for payload class: "
                            + payloadClass.getName()
            );
        }
        return extractor;
    }

    public Collection<BinaryReference> extract(Class<?> payloadClass, Object payload) {
        return requireExtractor(payloadClass).extractUntyped(payload);
    }
}
