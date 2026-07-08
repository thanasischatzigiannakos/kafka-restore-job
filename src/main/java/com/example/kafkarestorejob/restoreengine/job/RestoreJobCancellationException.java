package com.example.kafkarestorejob.restoreengine.job;

public class RestoreJobCancellationException extends RuntimeException {

    public RestoreJobCancellationException(String message) {
        super(message);
    }
}
