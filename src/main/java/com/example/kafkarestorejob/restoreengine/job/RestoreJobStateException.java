package com.example.kafkarestorejob.restoreengine.job;

public class RestoreJobStateException extends IllegalStateException {

    public RestoreJobStateException(String message) {
        super(message);
    }
}
