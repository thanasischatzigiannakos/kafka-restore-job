package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.verification.FileReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collection;
import java.util.List;

public abstract class AbstractJsonRestorePayloadHandler<T> implements RestorePayloadHandler {

    private final ObjectMapper objectMapper;

    protected AbstractJsonRestorePayloadHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public final Collection<FileReference> validateAndExtractFileReferences(
            RestoreRecordValidationContext context,
            byte[] payloadBytes
    ) {
        T payload = unpack(payloadBytes);
        validatePayloadType(context, payload);
        Collection<FileReference> references = extractFileReferences(payload);
        return references == null ? List.of() : references;
    }

    protected abstract Class<T> payloadClass();

    protected abstract void validatePayloadType(
            RestoreRecordValidationContext context,
            T payload
    );

    protected Collection<FileReference> extractFileReferences(T payload) {
        return List.of();
    }

    protected final T unpack(byte[] payloadBytes) {
        try {
            return objectMapper.readValue(payloadBytes, payloadClass());
        } catch (Exception exception) {
            throw new RestorePayloadValidationException(
                    "Failed to unpack " + restoreType() + " payload",
                    exception
            );
        }
    }
}
