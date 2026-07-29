package com.example.kafkarestorejob.restoreengine.validation;

public class MessageTypeMismatchException extends RestorePayloadValidationException {

    public MessageTypeMismatchException(String message) {
        super(message);
    }
}
