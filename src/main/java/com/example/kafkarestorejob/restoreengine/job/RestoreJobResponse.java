package com.example.kafkarestorejob.restoreengine.job;

import java.time.Instant;
import java.util.UUID;

/**
 * API response describing the current or final state of a restore job.
 *
 * @param id the restore job identifier
 * @param restoreType the logical restore type being executed
 * @param status the current lifecycle status
 * @param requestedAt the time at which the job was requested
 * @param startedAt the time at which execution started
 * @param completedAt the time at which execution completed or was cancelled
 * @param cancellationRequestedAt the time at which cancellation was requested, if any
 * @param restoreFromTimestamp the optional timestamp used to position the source consumer
 * @param updatedAt the last update timestamp for the job
 * @param sourceTopic the source topic, when known
 * @param targetTopic the target topic, when known
 * @param messageType the logical message-handler type, when known
 * @param errorMessage the terminal error message, when present
 * @param transactionsCommitted the number of committed transactions, when known
 * @param recordsRestored the number of restored records, when known
 */
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
        Integer transactionsCommitted,
        Long recordsRestored
) {
    /**
     * Builds an API response from the in-memory representation of a restore job.
     *
     * @param job the in-memory job
     * @return the API response
     */
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
                result == null ? null : result.transactionsCommitted(),
                result == null ? null : result.recordsRestored()
        );
    }
}
