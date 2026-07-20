package com.example.kafkarestorejob.restoreengine.verification;

public class BinaryChecksumMismatchException extends RuntimeException {

    public BinaryChecksumMismatchException(String message) {
        super(message);
    }
}
