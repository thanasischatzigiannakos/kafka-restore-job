package com.example.kafkarestorejob.restoreengine.serialization;

public interface RestoreMessageUnpacker<T> {

    String messageType();

    Class<T> payloadClass();

    T unpack(byte[] payload);
}
