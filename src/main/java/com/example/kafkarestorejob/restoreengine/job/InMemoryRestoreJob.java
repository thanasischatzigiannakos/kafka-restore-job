package com.example.kafkarestorejob.restoreengine.job;

import com.example.kafkarestorejob.restoreengine.kafka.RestoreExecutionResult;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;

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

    public Instant getStartedAt() {
        return startedAt.get();
    }

    public Instant getCompletedAt() {
        return completedAt.get();
    }

    public Instant getUpdatedAt() {
        return updatedAt.get();
    }

    public RestoreExecutionResult getExecutionResult() {
        return executionResult.get();
    }

    public String getErrorMessage() {
        return errorMessage.get();
    }

    public void markStarted() {
        updateTimestamps(startedAt);
    }

    public void recordExecutionResult(RestoreExecutionResult result) {
        executionResult.set(result);
        touch();
    }

    public void markCompleted() {
        updateTimestamps(completedAt);
    }

    public void markCancelled() {
        updateTimestamps(completedAt);
    }

    public void recordFailure(Throwable failure) {
        errorMessage.set(failure.getMessage());
        updateTimestamps(completedAt);
    }

    private void updateTimestamps(AtomicReference<Instant> timestampReference) {
        Instant now = Instant.now();
        timestampReference.compareAndSet(null, now);
        updatedAt.set(now);
    }

    private void touch() {
        updatedAt.set(Instant.now());
    }
}
