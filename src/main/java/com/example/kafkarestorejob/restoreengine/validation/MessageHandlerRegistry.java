package com.example.kafkarestorejob.restoreengine.validation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class MessageHandlerRegistry {

    private final Map<String, MessageHandler> handlersByType;

    public MessageHandlerRegistry(List<MessageHandler> handlers) {
        Map<String, MessageHandler> registry = new LinkedHashMap<>();
        for (MessageHandler handler : handlers) {
            MessageHandler previous = registry.put(handler.getType(), handler);
            if (previous != null) {
                throw new IllegalArgumentException(
                        "Duplicate message handler configured for type: " + handler.getType()
                );
            }
        }
        this.handlersByType = Map.copyOf(registry);
    }

    public MessageHandler requireHandler(String restoreType) {
        MessageHandler handler = handlersByType.get(restoreType);
        if (handler == null) {
            throw new IllegalArgumentException(
                    "No message handler configured for type: " + restoreType
            );
        }
        return handler;
    }
}
