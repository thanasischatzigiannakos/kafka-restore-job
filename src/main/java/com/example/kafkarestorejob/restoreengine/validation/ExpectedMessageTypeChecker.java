package com.example.kafkarestorejob.restoreengine.validation;

public interface ExpectedMessageTypeChecker {

    String messageType();

    void validate(RestoreRecordValidationContext context, byte[] payloadBytes);
}
