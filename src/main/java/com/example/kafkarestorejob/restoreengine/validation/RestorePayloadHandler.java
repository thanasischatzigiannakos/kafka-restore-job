package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.verification.FileReference;
import java.util.Collection;

public interface RestorePayloadHandler {

    String restoreType();

    Collection<FileReference> validateAndExtractFileReferences(
            RestoreRecordValidationContext context,
            byte[] payloadBytes
    );
}
