package com.example.kafkarestorejob.restoreengine.job;

/**
 * Raised when a restore job is cancelled before it can complete normally.
 */
public class RestoreJobCancellationException extends RuntimeException {

    /**
     * Creates the cancellation exception with a descriptive message.
     *
     * @param message the cancellation message
     */
    public RestoreJobCancellationException(String message) {
        super(message);
    }
}
