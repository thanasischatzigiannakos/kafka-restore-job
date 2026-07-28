package com.example.kafkarestorejob.restoreengine.validation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class RestorePayloadHandlerRegistry {

    private final Map<String, RestorePayloadHandler> handlersByRestoreType;

    public RestorePayloadHandlerRegistry(List<RestorePayloadHandler> handlers) {
        Map<String, RestorePayloadHandler> registry = new LinkedHashMap<>();
        for (RestorePayloadHandler handler : handlers) {
            RestorePayloadHandler previous = registry.put(handler.restoreType(), handler);
            if (previous != null) {
                throw new IllegalArgumentException(
                        "Duplicate restore payload handler configured for restore type: "
                                + handler.restoreType()
                );
            }
        }
        this.handlersByRestoreType = Map.copyOf(registry);
    }

    public RestorePayloadHandler requireHandler(String restoreType) {
        RestorePayloadHandler handler = handlersByRestoreType.get(restoreType);
        if (handler == null) {
            throw new IllegalArgumentException(
                    "No restore payload handler configured for restore type: " + restoreType
            );
        }
        return handler;
    }
}
