package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.serialization.NotificationRestoreMessageUnpacker;
import com.example.kafkarestorejob.restoreengine.serialization.model.NotificationRestoreMessage;
import org.springframework.stereotype.Component;

@Component
public class NotificationExpectedMessageTypeChecker implements ExpectedMessageTypeChecker {

    private final NotificationRestoreMessageUnpacker unpacker;

    public NotificationExpectedMessageTypeChecker(NotificationRestoreMessageUnpacker unpacker) {
        this.unpacker = unpacker;
    }

    @Override
    public String messageType() {
        return "notification";
    }

    @Override
    public void validate(RestoreRecordValidationContext context, byte[] payloadBytes) {
        NotificationRestoreMessage payload = unpacker.unpack(payloadBytes);
        if (!"notification".equals(payload.getEntityType())) {
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
