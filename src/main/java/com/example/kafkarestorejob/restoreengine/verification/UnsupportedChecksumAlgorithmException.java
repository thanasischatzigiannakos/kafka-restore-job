package com.example.kafkarestorejob.restoreengine.verification;

public class UnsupportedChecksumAlgorithmException extends RuntimeException {

    public UnsupportedChecksumAlgorithmException(String message) {
        super(message);
    }
}
