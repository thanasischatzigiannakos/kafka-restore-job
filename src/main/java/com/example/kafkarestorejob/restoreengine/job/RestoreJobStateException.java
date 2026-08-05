package com.example.kafkarestorejob.restoreengine.job;

/**
 * Raised when a restore job attempts an invalid lifecycle state transition.
 */
public class RestoreJobStateException extends IllegalStateException {

    /**
     * Creates the state exception with a descriptive message.
     *
     * @param message the state-transition error message
     */
    public RestoreJobStateException(String message) {
        super(message);
    }
}
