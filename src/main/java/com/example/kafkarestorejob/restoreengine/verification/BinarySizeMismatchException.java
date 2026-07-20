package com.example.kafkarestorejob.restoreengine.verification;

public class BinarySizeMismatchException extends RuntimeException {

    public BinarySizeMismatchException(String message) {
        super(message);
    }
}
