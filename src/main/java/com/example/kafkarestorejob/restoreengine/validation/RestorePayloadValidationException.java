package com.example.kafkarestorejob.restoreengine.validation;

/**
 * Raised when a restore payload fails parsing, type validation, or binary validation.
 */
public class RestorePayloadValidationException extends RuntimeException {

    /**
     * Creates the validation exception with a message.
     *
     * @param message the validation failure message
     */
    public RestorePayloadValidationException(String message) {
        super(message);
    }

    /**
     * Creates the validation exception with a message and cause.
     *
     * @param message the validation failure message
     * @param cause the root cause
     */
    public RestorePayloadValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
