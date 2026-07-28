package com.example.kafkarestorejob.restoreengine.validation;

public interface RestorePayloadHandler {

    String restoreType();

    void validate(
            RestoreRecordValidationContext context,
            byte[] payloadBytes
    );
}
