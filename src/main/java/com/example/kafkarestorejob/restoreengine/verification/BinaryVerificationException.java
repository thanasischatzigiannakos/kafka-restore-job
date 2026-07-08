package com.example.kafkarestorejob.restoreengine.verification;

public class BinaryVerificationException extends RuntimeException {

    public BinaryVerificationException(String message, Throwable cause) {
        super(message, cause);
    }
}
