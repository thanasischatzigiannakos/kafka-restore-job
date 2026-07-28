package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.serialization.model.NotificationRestoreMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class NotificationRestorePayloadHandler
        extends AbstractJsonRestorePayloadHandler<NotificationRestoreMessage> {

    public NotificationRestorePayloadHandler(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    public String restoreType() {
        return "notification";
    }

    @Override
    protected Class<NotificationRestoreMessage> payloadClass() {
        return NotificationRestoreMessage.class;
    }

    @Override
    protected void validatePayloadType(
            RestoreRecordValidationContext context,
            NotificationRestoreMessage payload
    ) {
        if (payload.getEntityType() == null || !"notification".equalsIgnoreCase(payload.getEntityType())) {
            throw new RestorePayloadValidationException(
                    "Unexpected payload type for restoreType=" + context.restoreType()
                            + " sourceTopic=" + context.sourceTopic()
                            + " partition=" + context.partition()
                            + " offset=" + context.offset()
                            + " expectedMessageType=notification"
                            + " actualEntityType=" + payload.getEntityType()
            );
        }
    }
}
