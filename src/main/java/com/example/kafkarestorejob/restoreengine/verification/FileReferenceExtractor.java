package com.example.kafkarestorejob.restoreengine.verification;

import java.util.Collection;

public interface FileReferenceExtractor<T> {

    Class<T> payloadClass();

    Collection<BinaryReference> extract(T payload);

    default Collection<BinaryReference> extractUntyped(Object payload) {
        return extract(payloadClass().cast(payload));
    }
}
