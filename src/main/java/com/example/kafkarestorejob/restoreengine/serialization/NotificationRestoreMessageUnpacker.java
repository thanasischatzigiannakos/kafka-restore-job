package com.example.kafkarestorejob.restoreengine.serialization;

import com.example.kafkarestorejob.restoreengine.serialization.model.NotificationRestoreMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class NotificationRestoreMessageUnpacker
        extends AbstractJsonRestoreMessageUnpacker<NotificationRestoreMessage> {

    public NotificationRestoreMessageUnpacker(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    public String messageType() {
        return "notification";
    }

    @Override
    public Class<NotificationRestoreMessage> payloadClass() {
        return NotificationRestoreMessage.class;
    }
}
