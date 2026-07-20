package com.example.kafkarestorejob.restoreengine.verification;

public class BinaryNotFoundException extends RuntimeException {

    public BinaryNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
