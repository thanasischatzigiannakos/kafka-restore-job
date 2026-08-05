package com.example.kafkarestorejob.restoreengine.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.jupiter.api.Test;

class MessageHandlerRegistryTest {

    @Test
    void resolvesRegisteredHandlerByType() {
        MessageHandler handler = mock(MessageHandler.class);
        when(handler.getType()).thenReturn("application");
        MessageHandlerRegistry registry = new MessageHandlerRegistry(List.of(handler));

        assertEquals(handler, registry.requireHandler("application"));
    }

    @Test
    void rejectsDuplicateHandlerTypes() {
        MessageHandler first = mock(MessageHandler.class);
        MessageHandler second = mock(MessageHandler.class);
        when(first.getType()).thenReturn("application");
        when(second.getType()).thenReturn("application");

        assertThrows(IllegalArgumentException.class, () -> new MessageHandlerRegistry(List.of(first, second)));
    }

    @Test
    void rejectsUnknownHandlerType() {
        MessageHandler handler = mock(MessageHandler.class);
        when(handler.getType()).thenReturn("application");
        MessageHandlerRegistry registry = new MessageHandlerRegistry(List.of(handler));

        assertThrows(IllegalArgumentException.class, () -> registry.requireHandler("missing"));
    }
}
