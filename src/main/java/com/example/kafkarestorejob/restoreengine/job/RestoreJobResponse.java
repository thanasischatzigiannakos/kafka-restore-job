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
        Instant restoreFromTimestamp,
        Instant updatedAt,
        String sourceTopic,
        String targetTopic,
        String messageType,
        String errorMessage,
        Integer batchesCommitted,
        Long recordsRestored
) {
    public static RestoreJobResponse fromEntity(RestoreJobEntity entity) {
        return fromEntity(entity, null);
    }

    public static RestoreJobResponse fromEntity(RestoreJobEntity entity, RunningRestoreJob runningRestoreJob) {
        RestoreJobStatus status = runningRestoreJob == null ? entity.getStatus() : runningRestoreJob.getStatus();
        Instant cancellationRequestedAt = runningRestoreJob == null
                ? entity.getCancellationRequestedAt()
                : runningRestoreJob.getCancellationRequestedAt();
        return new RestoreJobResponse(
                entity.getId(),
                entity.getRestoreType(),
                status,
                entity.getRequestedAt(),
                entity.getStartedAt(),
                entity.getCompletedAt(),
                cancellationRequestedAt,
                entity.getRestoreFromTimestamp(),
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
