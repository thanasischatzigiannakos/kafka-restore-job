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
    public static RestoreJobResponse fromContext(RestoreJobExecutionContext context) {
        var result = context.getExecutionResult();
        return new RestoreJobResponse(
                context.getJobId(),
                context.getRestoreType(),
                context.getStatus(),
                context.getRequestedAt(),
                context.getStartedAt(),
                context.getCompletedAt(),
                context.getCancellationRequestedAt(),
                context.getRestoreFromTimestamp(),
                context.getUpdatedAt(),
                result == null ? null : result.sourceTopic(),
                result == null ? null : result.targetTopic(),
                result == null ? null : result.messageType(),
                context.getErrorMessage(),
                result == null ? null : result.batchesCommitted(),
                result == null ? null : result.recordsRestored()
        );
    }
}
