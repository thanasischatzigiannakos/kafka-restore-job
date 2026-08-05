package com.example.kafkarestorejob.restoreengine.validation;

import java.util.UUID;

/**
 * Carries the minimum record metadata required for validation diagnostics.
 *
 * @param jobId the restore job id
 * @param restoreType the logical restore type
 * @param sourceTopic the source topic being read
 * @param partition the source partition
 * @param offset the source offset
 */
public record RestoreRecordValidationContext(
        UUID jobId,
        String restoreType,
        String sourceTopic,
        int partition,
        long offset
) {
}
