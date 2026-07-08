package com.example.kafkarestorejob.restoreengine.verification;

import java.util.List;

public interface BinaryReferenceExtractor<T> {

    String messageType();

    Class<T> payloadClass();

    List<BinaryReference> extract(T payload);

    default List<BinaryReference> extractUntyped(Object payload) {
        return extract(payloadClass().cast(payload));
    }
}
