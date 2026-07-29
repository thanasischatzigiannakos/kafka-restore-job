package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.verification.FileReference;
import com.example.kafkarestorejob.restoreengine.verification.S3FileExistenceVerifier;
import java.util.Collection;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public abstract class BinaryMessageHandler<T> extends AbstractMessageHandler<T> {

    private final S3FileExistenceVerifier s3FileExistenceVerifier;

    protected BinaryMessageHandler(S3FileExistenceVerifier s3FileExistenceVerifier) {
        this.s3FileExistenceVerifier = s3FileExistenceVerifier;
    }

    @Override
    public final boolean hasBinary() {
        return true;
    }

    @Override
    protected final void validateBinary(
            ConsumerRecord<String, byte[]> sourceRecord,
            RestoreRecordValidationContext context,
            T payload
    ) {
        Collection<FileReference> references = extractFileReferences(payload);
        for (FileReference reference : references == null ? List.<FileReference>of() : references) {
            s3FileExistenceVerifier.verifyExists(context, getType(), reference);
        }
    }

    protected abstract Collection<FileReference> extractFileReferences(T payload);
}
