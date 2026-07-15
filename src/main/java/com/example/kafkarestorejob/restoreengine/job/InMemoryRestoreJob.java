package com.example.kafkarestorejob.restoreengine.job;

import com.example.kafkarestorejob.restoreengine.kafka.RestoreExecutionResult;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

final class InMemoryRestoreJob {

    private final UUID jobId;
    private final String restoreType;
    private final Instant requestedAt;
    private final Instant restoreFromTimestamp;
    private final RestoreJobExecutionContext context;
    private final AtomicReference<Instant> startedAt = new AtomicReference<>();
    private final AtomicReference<Instant> completedAt = new AtomicReference<>();
    private final AtomicReference<Instant> updatedAt = new AtomicReference<>();
    private final AtomicReference<RestoreExecutionResult> executionResult = new AtomicReference<>();
    private final AtomicReference<String> errorMessage = new AtomicReference<>();

    InMemoryRestoreJob(
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

    UUID getJobId() {
        return jobId;
    }

    String getRestoreType() {
        return restoreType;
    }

    Instant getRequestedAt() {
        return requestedAt;
    }

    Instant getRestoreFromTimestamp() {
        return restoreFromTimestamp;
    }

    RestoreJobExecutionContext getContext() {
        return context;
    }

    Instant getStartedAt() {
        return startedAt.get();
    }

    Instant getCompletedAt() {
        return completedAt.get();
    }

    Instant getUpdatedAt() {
        return updatedAt.get();
    }

    RestoreExecutionResult getExecutionResult() {
        return executionResult.get();
    }

    String getErrorMessage() {
        return errorMessage.get();
    }

    void markStarted() {
        Instant now = Instant.now();
        startedAt.compareAndSet(null, now);
        updatedAt.set(now);
    }

    void recordExecutionResult(RestoreExecutionResult result) {
        executionResult.set(result);
        updatedAt.set(Instant.now());
    }

    void markCompleted() {
        Instant now = Instant.now();
        completedAt.compareAndSet(null, now);
        updatedAt.set(now);
    }

    void markCancelled() {
        Instant now = Instant.now();
        completedAt.compareAndSet(null, now);
        updatedAt.set(now);
    }

    void recordFailure(Throwable failure) {
        errorMessage.set(failure.getMessage());
        Instant now = Instant.now();
        completedAt.compareAndSet(null, now);
        updatedAt.set(now);
    }
}
