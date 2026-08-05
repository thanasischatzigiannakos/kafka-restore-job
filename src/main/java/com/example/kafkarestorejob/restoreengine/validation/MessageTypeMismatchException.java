package com.example.kafkarestorejob.restoreengine.validation;

/**
 * Raised when a payload parses successfully but does not match the expected logical message type.
 */
public class MessageTypeMismatchException extends RestorePayloadValidationException {

    /**
     * Creates the mismatch exception with a descriptive message.
     *
     * @param message the mismatch description
     */
    public MessageTypeMismatchException(String message) {
        super(message);
    }
}
