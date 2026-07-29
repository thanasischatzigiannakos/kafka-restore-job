package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.serialization.model.NotificationRestoreMessage;
import org.springframework.stereotype.Component;

@Component
public class NotificationMessageHandler extends AbstractMessageHandler<NotificationRestoreMessage> {

    @Override
    public String getType() {
        return "notification";
    }

    @Override
    protected NotificationRestoreMessage parse(byte[] payloadBytes) {
        try {
            return NotificationRestoreMessage.parseFrom(payloadBytes);
        } catch (Exception exception) {
            throw parsingFailure(exception);
        }
    }

    @Override
    protected boolean validateStructure(NotificationRestoreMessage payload) {
        return payload.getEntityType() != null
                && "notification".equalsIgnoreCase(payload.getEntityType());
    }

    @Override
    protected String payloadDescription(NotificationRestoreMessage payload) {
        return "entityType=" + payload.getEntityType();
    }
}
