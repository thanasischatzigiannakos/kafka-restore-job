package com.example.kafkarestorejob.restoreengine.job;

import com.example.kafkarestorejob.restoreengine.kafka.RestoreExecutionResult;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;

/**
 * In-memory representation of a restore job used by the coordinator while the application is
 * running.
 */
public final class InMemoryRestoreJob {

    @Getter
    private final UUID jobId;
    @Getter
    private final String restoreType;
    @Getter
    private final Instant requestedAt;
    @Getter
    private final Instant restoreFromTimestamp;
    @Getter
    private final RestoreJobExecutionContext context;
    private final AtomicReference<Instant> startedAt = new AtomicReference<>();
    private final AtomicReference<Instant> completedAt = new AtomicReference<>();
    private final AtomicReference<Instant> updatedAt = new AtomicReference<>();
    private final AtomicReference<RestoreExecutionResult> executionResult = new AtomicReference<>();
    private final AtomicReference<String> errorMessage = new AtomicReference<>();

    /**
     * Creates the in-memory restore job state.
     *
     * @param jobId the restore job identifier
     * @param restoreType the logical restore type
     * @param requestedAt the time at which the job was requested
     * @param restoreFromTimestamp the optional timestamp used to position the source consumer
     * @param context the mutable execution context
     */
    public InMemoryRestoreJob(
            UUID jobId,
            String restoreType,
            Instant requestedAt,
            Instant restoreFromTimestamp,
            RestoreJobExecutionContext context
    ) {
        this.jobId = jobId;
        this.restoreType = restoreType;
        this.requestedAt = requestedAt;
        this.restoreFromTimestamp = restoreFromTimestamp;
        this.context = context;
        this.updatedAt.set(requestedAt);
    }

    /**
     * Returns the timestamp at which execution started.
     *
     * @return the start timestamp
     */
    public Instant getStartedAt() {
        return startedAt.get();
    }

    /**
     * Returns the timestamp at which execution completed or was cancelled.
     *
     * @return the completion timestamp
     */
    public Instant getCompletedAt() {
        return completedAt.get();
    }

    /**
     * Returns the timestamp of the most recent in-memory update.
     *
     * @return the update timestamp
     */
    public Instant getUpdatedAt() {
        return updatedAt.get();
    }

    /**
     * Returns the execution result, when the restore loop has produced one.
     *
     * @return the execution result
     */
    public RestoreExecutionResult getExecutionResult() {
        return executionResult.get();
    }

    /**
     * Returns the terminal error message, when one has been recorded.
     *
     * @return the error message
     */
    public String getErrorMessage() {
        return errorMessage.get();
    }

    /**
     * Marks the in-memory job as started and refreshes its update timestamp.
     */
    public void markStarted() {
        updateTimestamps(startedAt);
    }

    /**
     * Records the execution result and refreshes the update timestamp.
     *
     * @param result the execution result
     */
    public void recordExecutionResult(RestoreExecutionResult result) {
        executionResult.set(result);
        touch();
    }

    /**
     * Marks the in-memory job as completed and refreshes its update timestamp.
     */
    public void markCompleted() {
        updateTimestamps(completedAt);
    }

    /**
     * Marks the in-memory job as cancelled and refreshes its update timestamp.
     */
    public void markCancelled() {
        updateTimestamps(completedAt);
    }

    /**
     * Records a terminal failure and refreshes the completion and update timestamps.
     *
     * @param failure the failure that terminated the job
     */
    public void recordFailure(Throwable failure) {
        errorMessage.set(failure.getMessage());
        updateTimestamps(completedAt);
    }

    /**
     * Sets the supplied timestamp reference once and always refreshes the last-updated timestamp.
     *
     * @param timestampReference the timestamp reference to initialize
     */
    private void updateTimestamps(AtomicReference<Instant> timestampReference) {
        Instant now = Instant.now();
        timestampReference.compareAndSet(null, now);
        updatedAt.set(now);
    }

    /**
     * Refreshes the last-updated timestamp without touching any lifecycle milestone.
     */
    private void touch() {
        updatedAt.set(Instant.now());
    }
}
