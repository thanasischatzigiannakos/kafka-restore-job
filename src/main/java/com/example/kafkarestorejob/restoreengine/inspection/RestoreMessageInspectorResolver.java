package com.example.kafkarestorejob.restoreengine.inspection;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class RestoreMessageInspectorResolver {

    private final Map<String, RestoreMessageInspector<?>> inspectorsByType;

    public RestoreMessageInspectorResolver(List<RestoreMessageInspector<?>> inspectors) {
        Map<String, RestoreMessageInspector<?>> registry = new LinkedHashMap<>();
        for (RestoreMessageInspector<?> inspector : inspectors) {
            RestoreMessageInspector<?> previous = registry.put(inspector.messageType(), inspector);
            if (previous != null) {
                throw new IllegalArgumentException(
                        "Duplicate inspector configured for message type: " + inspector.messageType()
                );
            }
        }
        this.inspectorsByType = Map.copyOf(registry);
    }

    public RestoreMessageInspector<?> resolve(String messageType) {
        RestoreMessageInspector<?> inspector = inspectorsByType.get(messageType);
        if (inspector == null) {
            throw new IllegalArgumentException("No inspector configured for message type: " + messageType);
        }
        return inspector;
    }
}
