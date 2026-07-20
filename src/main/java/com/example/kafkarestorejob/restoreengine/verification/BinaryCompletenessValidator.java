package com.example.kafkarestorejob.restoreengine.verification;

import com.example.kafkarestorejob.restoreengine.validation.RestoreRecordValidationContext;

public interface BinaryCompletenessValidator {

    void validate(
            RestoreRecordValidationContext context,
            String configuredMessageType,
            Class<?> payloadClass,
            BinaryReference reference
    );
}
