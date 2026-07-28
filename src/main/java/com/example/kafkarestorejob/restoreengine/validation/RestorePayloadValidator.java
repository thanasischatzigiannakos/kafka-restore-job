package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.verification.FileReference;
import com.example.kafkarestorejob.restoreengine.verification.S3FileExistenceVerifier;
import java.util.Collection;
import org.springframework.stereotype.Component;

@Component
public class RestorePayloadValidator {

    private final RestorePayloadHandlerRegistry handlerRegistry;
    private final S3FileExistenceVerifier s3FileExistenceVerifier;

    public RestorePayloadValidator(
            RestorePayloadHandlerRegistry handlerRegistry,
            S3FileExistenceVerifier s3FileExistenceVerifier
    ) {
        this.handlerRegistry = handlerRegistry;
        this.s3FileExistenceVerifier = s3FileExistenceVerifier;
    }

    public void validate(
            RestoreRecordValidationContext context,
            byte[] payloadBytes
    ) {
        RestorePayloadHandler handler = handlerRegistry.requireHandler(context.restoreType());
        Collection<FileReference> references =
                handler.validateAndExtractFileReferences(context, payloadBytes);
        for (FileReference reference : references) {
            s3FileExistenceVerifier.verifyExists(context, context.restoreType(), reference);
        }
    }
}
