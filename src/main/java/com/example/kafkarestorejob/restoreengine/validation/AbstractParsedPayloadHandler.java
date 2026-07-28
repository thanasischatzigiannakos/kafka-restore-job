package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.verification.FileReference;
import com.example.kafkarestorejob.restoreengine.verification.S3FileExistenceVerifier;
import java.util.Collection;
import java.util.List;

public abstract class AbstractParsedPayloadHandler<T> implements RestorePayloadHandler {

    private final S3FileExistenceVerifier s3FileExistenceVerifier;

    protected AbstractParsedPayloadHandler(S3FileExistenceVerifier s3FileExistenceVerifier) {
        this.s3FileExistenceVerifier = s3FileExistenceVerifier;
    }

    @Override
    public final void validate(
            RestoreRecordValidationContext context,
            byte[] payloadBytes
    ) {
        T payload = parse(payloadBytes);
        validatePayloadType(context, payload);
        Collection<FileReference> references = extractFileReferences(payload);
        for (FileReference reference : references == null ? List.<FileReference>of() : references) {
            s3FileExistenceVerifier.verifyExists(context, context.restoreType(), reference);
        }
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
