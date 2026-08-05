package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.serialization.model.NotificationRestoreMessage;
import org.springframework.stereotype.Component;

/**
 * Validates notification payloads that do not require any external binary lookup.
 */
@Component
public class NotificationMessageHandler extends AbstractMessageHandler<NotificationRestoreMessage> {

    /**
     * Returns the logical restore type handled by this validator.
     *
     * @return {@code notification}
     */
    @Override
    public String getType() {
        return "notification";
    }

    /**
     * Parses the placeholder notification payload model.
     *
     * @param payloadBytes the serialized notification payload
     * @return the parsed notification payload
     */
    @Override
    protected NotificationRestoreMessage parse(byte[] payloadBytes) {
        try {
            return NotificationRestoreMessage.parseFrom(payloadBytes);
        } catch (Exception exception) {
            throw parsingFailure(exception);
        }
    }

    /**
     * Checks that the parsed payload advertises the expected notification entity type.
     *
     * @param payload the parsed payload
     * @return {@code true} when the payload matches the notification restore type
     */
    @Override
    protected boolean validateStructure(NotificationRestoreMessage payload) {
        return payload.getEntityType() != null
                && "notification".equalsIgnoreCase(payload.getEntityType());
    }

    /**
     * Builds a concise mismatch description for diagnostics.
     *
     * @param payload the parsed payload
     * @return the entity-type description
     */
    @Override
    protected String payloadDescription(NotificationRestoreMessage payload) {
        return "entityType=" + payload.getEntityType();
    }
}
