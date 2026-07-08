package com.example.kafkarestorejob.restoreengine.job;

import java.time.Instant;
import java.util.UUID;

public record RestoreJobResponse(
        UUID id,
        String restoreType,
        RestoreJobStatus status,
        Instant requestedAt,
        Instant startedAt,
        Instant completedAt,
        Instant cancellationRequestedAt,
        Instant updatedAt,
        String sourceTopic,
        String targetTopic,
        String messageType,
        String errorMessage,
        Integer batchesCommitted,
        Long recordsRestored
) {
    public static RestoreJobResponse fromEntity(RestoreJobEntity entity) {
        return new RestoreJobResponse(
                entity.getId(),
                entity.getRestoreType(),
                entity.getStatus(),
                entity.getRequestedAt(),
                entity.getStartedAt(),
                entity.getCompletedAt(),
                entity.getCancellationRequestedAt(),
                entity.getUpdatedAt(),
                entity.getSourceTopic(),
                entity.getTargetTopic(),
                entity.getMessageType(),
                entity.getErrorMessage(),
                entity.getBatchesCommitted(),
                entity.getRecordsRestored()
        );
    }
}
