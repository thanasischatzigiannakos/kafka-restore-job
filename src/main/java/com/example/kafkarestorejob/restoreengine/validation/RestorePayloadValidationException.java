package com.example.kafkarestorejob.restoreengine.validation;

public class RestorePayloadValidationException extends RuntimeException {

    public RestorePayloadValidationException(String message) {
        super(message);
    }

    public RestorePayloadValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
