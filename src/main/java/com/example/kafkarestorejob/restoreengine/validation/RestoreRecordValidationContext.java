package com.example.kafkarestorejob.restoreengine.validation;

import java.util.UUID;

public record RestoreRecordValidationContext(
        UUID jobId,
        String restoreType,
        String sourceTopic,
        int partition,
        long offset
) {
}
