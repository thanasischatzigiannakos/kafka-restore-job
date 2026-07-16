package com.example.kafkarestorejob.restoreengine.inspection;

public interface RestoreMessageInspector<T> {

    String messageType();

    Class<T> payloadClass();

    void inspect(String restoreType, T payload);

    default void inspectUntyped(String restoreType, Object payload) {
        inspect(restoreType, payloadClass().cast(payload));
    }
}
