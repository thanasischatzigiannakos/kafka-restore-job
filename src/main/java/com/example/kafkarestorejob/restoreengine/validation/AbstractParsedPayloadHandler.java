package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.verification.FileReference;
import java.util.Collection;
import java.util.List;

public abstract class AbstractParsedPayloadHandler<T> implements RestorePayloadHandler {

    @Override
    public final Collection<FileReference> validateAndExtractFileReferences(
            RestoreRecordValidationContext context,
            byte[] payloadBytes
    ) {
        T payload = parse(payloadBytes);
        validatePayloadType(context, payload);
        Collection<FileReference> references = extractFileReferences(payload);
        return references == null ? List.of() : references;
    }

    protected abstract T parse(byte[] payloadBytes);

    protected abstract void validatePayloadType(
            RestoreRecordValidationContext context,
            T payload
    );

    protected Collection<FileReference> extractFileReferences(T payload) {
        return List.of();
    }

    protected final RestorePayloadValidationException parsingFailure(
            Exception exception
    ) {
        return new RestorePayloadValidationException(
                "Failed to parse " + restoreType() + " payload",
                exception
        );
    }
}
