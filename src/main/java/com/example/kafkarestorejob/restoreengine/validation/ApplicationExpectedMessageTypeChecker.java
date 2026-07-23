package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.serialization.ApplicationRestoreMessageUnpacker;
import com.example.kafkarestorejob.restoreengine.serialization.model.ApplicationRestoreMessage;
import org.springframework.stereotype.Component;

@Component
public class ApplicationExpectedMessageTypeChecker implements ExpectedMessageTypeChecker {

    private final ApplicationRestoreMessageUnpacker unpacker;

    public ApplicationExpectedMessageTypeChecker(ApplicationRestoreMessageUnpacker unpacker) {
        this.unpacker = unpacker;
    }

    @Override
    public String messageType() {
        return "application";
    }

    @Override
    public void validate(RestoreRecordValidationContext context, byte[] payloadBytes) {
        ApplicationRestoreMessage payload = unpacker.unpack(payloadBytes);
        if (payload.getEntityType() == null || !"application".equalsIgnoreCase(payload.getEntityType())) {
            throw new RestorePayloadValidationException(
                    "Unexpected payload type for restoreType=" + context.restoreType()
                            + " sourceTopic=" + context.sourceTopic()
                            + " partition=" + context.partition()
                            + " offset=" + context.offset()
                            + " expectedMessageType=application"
                            + " actualEntityType=" + payload.getEntityType()
            );
        }
    }
}
