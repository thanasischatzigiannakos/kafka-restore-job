package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.serialization.model.NotificationRestoreMessage;
import org.springframework.stereotype.Component;

@Component
public class NotificationRestorePayloadHandler
        extends AbstractParsedPayloadHandler<NotificationRestoreMessage> {

    public NotificationRestorePayloadHandler() {
    }

    @Override
    public String restoreType() {
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
