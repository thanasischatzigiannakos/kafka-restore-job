package com.example.kafkarestorejob.restoreengine.kafka;

/**
 * Top-level runtime exception used for restore-loop failures.
 */
public class RestoreEngineException extends RuntimeException {

    /**
     * Creates the exception with a message.
     *
     * @param message the failure message
     */
    public RestoreEngineException(String message) {
        super(message);
    }

    /**
     * Creates the exception with a message and root cause.
     *
     * @param message the failure message
     * @param cause the underlying cause
     */
    public RestoreEngineException(String message, Throwable cause) {
        super(message, cause);
    }
}
