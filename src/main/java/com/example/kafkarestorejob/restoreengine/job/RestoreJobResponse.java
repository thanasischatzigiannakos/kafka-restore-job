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
    public static RestoreJobResponse fromInMemoryJob(InMemoryRestoreJob job) {
        var context = job.getContext();
        var result = job.getExecutionResult();
        return new RestoreJobResponse(
                job.getJobId(),
                job.getRestoreType(),
                context.getStatus(),
                job.getRequestedAt(),
                job.getStartedAt(),
                job.getCompletedAt(),
                context.getCancellationRequestedAt(),
                job.getRestoreFromTimestamp(),
                job.getUpdatedAt(),
                result == null ? null : result.sourceTopic(),
                result == null ? null : result.targetTopic(),
                result == null ? null : result.messageType(),
                job.getErrorMessage(),
                result == null ? null : result.batchesCommitted(),
                result == null ? null : result.recordsRestored()
        );
    }
}
