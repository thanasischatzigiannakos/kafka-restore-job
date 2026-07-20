package com.example.kafkarestorejob.restoreengine.validation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class FileCapablePayloadRegistry {

    private final Map<Class<?>, FileCapablePayloadType> payloadTypesByClass;

    public FileCapablePayloadRegistry(List<FileCapablePayloadType> payloadTypes) {
        Map<Class<?>, FileCapablePayloadType> registry = new LinkedHashMap<>();
        for (FileCapablePayloadType payloadType : payloadTypes) {
            FileCapablePayloadType previous = registry.put(payloadType.payloadClass(), payloadType);
            if (previous != null) {
                throw new IllegalArgumentException(
                        "Duplicate file-capable payload type configured for class: "
                                + payloadType.payloadClass().getName()
                );
            }
        }
        this.payloadTypesByClass = Map.copyOf(registry);
    }

    public boolean supports(Class<?> payloadClass) {
        return payloadTypesByClass.containsKey(payloadClass);
    }
}
