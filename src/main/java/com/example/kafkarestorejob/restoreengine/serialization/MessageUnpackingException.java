package com.example.kafkarestorejob.restoreengine.serialization;

public class MessageUnpackingException extends RuntimeException {

    public MessageUnpackingException(String message, Throwable cause) {
        super(message, cause);
    }
}
