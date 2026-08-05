package com.example.kafkarestorejob.restoreengine.validation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Resolves message handlers by logical restore type.
 */
@Component
public class MessageHandlerRegistry {

    private final Map<String, MessageHandler> handlersByType;

    /**
     * Builds an immutable handler registry keyed by handler type and rejects duplicates eagerly.
     *
     * @param handlers the discovered handler beans
     */
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

    /**
     * Returns the handler registered for the supplied restore type.
     *
     * @param restoreType the restore type to resolve
     * @return the matching handler
     * @throws IllegalArgumentException when no handler is registered for the type
     */
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
