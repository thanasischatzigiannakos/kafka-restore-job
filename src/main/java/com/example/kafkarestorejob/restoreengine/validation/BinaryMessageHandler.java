package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.verification.FileReference;
import com.example.kafkarestorejob.restoreengine.verification.S3FileExistenceVerifier;
import java.util.Collection;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Base handler for payloads that may reference external binary content in S3.
 *
 * @param <T> the parsed payload type
 */
public abstract class BinaryMessageHandler<T> extends AbstractMessageHandler<T> {

    private final S3FileExistenceVerifier s3FileExistenceVerifier;

    protected BinaryMessageHandler(S3FileExistenceVerifier s3FileExistenceVerifier) {
        this.s3FileExistenceVerifier = s3FileExistenceVerifier;
    }

    /**
     * Indicates that binary-capable handlers must run the S3-backed validation path.
     *
     * @return always {@code true}
     */
    @Override
    public final boolean hasBinary() {
        return true;
    }

    /**
     * Extracts all known binary references from the parsed payload and validates each one against
     * the configured S3-backed verifier.
     *
     * @param sourceRecord the source record being validated
     * @param context the restore context used for diagnostics
     * @param payload the parsed payload
     */
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

    /**
     * Extracts all binary references that must exist for the supplied payload.
     *
     * @param payload the parsed payload
     * @return the binary references to validate
     */
    protected abstract Collection<FileReference> extractFileReferences(T payload);
}
