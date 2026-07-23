package com.example.kafkarestorejob.restoreengine.verification;

import java.util.Collection;

public interface FileReferenceExtractor<T> {

    Class<T> payloadClass();

    Collection<FileReference> extract(T payload);

    default Collection<FileReference> extractUntyped(Object payload) {
        return extract(payloadClass().cast(payload));
    }
}
