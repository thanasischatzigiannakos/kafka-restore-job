package com.example.kafkarestorejob.restoreengine.kafka;

public class RestoreEngineException extends RuntimeException {

    public RestoreEngineException(String message) {
        super(message);
    }

    public RestoreEngineException(String message, Throwable cause) {
        super(message, cause);
    }
}
