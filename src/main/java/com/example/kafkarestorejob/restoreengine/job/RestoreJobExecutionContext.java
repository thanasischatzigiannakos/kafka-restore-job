package com.example.kafkarestorejob.restoreengine.job;

import com.example.kafkarestorejob.restoreengine.kafka.RestoreExecutionResult;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public final class RestoreJobExecutionContext {

    private final UUID jobId;
    private final String restoreType;
    private final Instant requestedAt;
    private final Instant restoreFromTimestamp;
    private final AtomicReference<RestoreJobStatus> status =
            new AtomicReference<>(RestoreJobStatus.PENDING);
    private final AtomicReference<Instant> cancellationRequestedAt = new AtomicReference<>();
    private final AtomicReference<Instant> startedAt = new AtomicReference<>();
    private final AtomicReference<Instant> completedAt = new AtomicReference<>();
    private final AtomicReference<Instant> updatedAt = new AtomicReference<>();
    private final AtomicReference<RestoreExecutionResult> executionResult = new AtomicReference<>();
    private final AtomicReference<String> errorMessage = new AtomicReference<>();

    public RestoreJobExecutionContext(
            UUID jobId,
            String restoreType,
            Instant requestedAt,
            Instant restoreFromTimestamp
    ) {
        this.jobId = jobId;
        this.restoreType = restoreType;
        this.requestedAt = requestedAt;
        this.restoreFromTimestamp = restoreFromTimestamp;
        this.updatedAt.set(requestedAt);
    }

    public UUID getJobId() {
        return jobId;
    }

    public String getRestoreType() {
        return restoreType;
    }

    public Instant getRequestedAt() {
        return requestedAt;
    }

    public Instant getRestoreFromTimestamp() {
        return restoreFromTimestamp;
    }

    public RestoreJobStatus getStatus() {
        return status.get();
    }

    public Instant getCancellationRequestedAt() {
        return cancellationRequestedAt.get();
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

    public void markRunning() {
        if (status.compareAndSet(RestoreJobStatus.PENDING, RestoreJobStatus.RUNNING)) {
            Instant now = Instant.now();
            startedAt.compareAndSet(null, now);
            updatedAt.set(now);
            return;
        }

        RestoreJobStatus currentStatus = status.get();
        if (currentStatus == RestoreJobStatus.CANCELLATION_REQUESTED) {
            throw new RestoreJobCancellationException(
                    "Restore job cancellation was requested before execution started. jobId=" + jobId
            );
        }

        throw invalidTransition(currentStatus, RestoreJobStatus.RUNNING);
    }

    public boolean requestCancellation() {
        while (true) {
            RestoreJobStatus currentStatus = status.get();
            if (currentStatus == RestoreJobStatus.CANCELLATION_REQUESTED) {
                return true;
            }

            if (currentStatus != RestoreJobStatus.PENDING
                    && currentStatus != RestoreJobStatus.RUNNING) {
                return false;
            }

            if (status.compareAndSet(currentStatus, RestoreJobStatus.CANCELLATION_REQUESTED)) {
                Instant now = Instant.now();
                cancellationRequestedAt.compareAndSet(null, now);
                updatedAt.set(now);
                return true;
            }
        }
    }

    public boolean isCancellationRequested() {
        RestoreJobStatus currentStatus = status.get();
        return currentStatus == RestoreJobStatus.CANCELLATION_REQUESTED
                || currentStatus == RestoreJobStatus.CANCELLED;
    }

    public void throwIfCancellationRequested() {
        if (isCancellationRequested()) {
            throw new RestoreJobCancellationException(
                    "Cancellation requested for running restore job. jobId=" + jobId
            );
        }
    }

    public boolean tryMarkFinalizing() {
        if (status.compareAndSet(RestoreJobStatus.RUNNING, RestoreJobStatus.FINALIZING)) {
            updatedAt.set(Instant.now());
            return true;
        }
        return false;
    }

    public void markCompleted() {
        if (status.compareAndSet(RestoreJobStatus.FINALIZING, RestoreJobStatus.COMPLETED)) {
            Instant now = Instant.now();
            completedAt.compareAndSet(null, now);
            updatedAt.set(now);
            return;
        }

        throw invalidTransition(status.get(), RestoreJobStatus.COMPLETED);
    }

    public void markCancelled() {
        if (status.compareAndSet(
                RestoreJobStatus.CANCELLATION_REQUESTED,
                RestoreJobStatus.CANCELLED)) {
            Instant now = Instant.now();
            completedAt.compareAndSet(null, now);
            updatedAt.set(now);
            return;
        }

        RestoreJobStatus currentStatus = status.get();
        if (currentStatus == RestoreJobStatus.CANCELLED) {
            return;
        }

        throw invalidTransition(currentStatus, RestoreJobStatus.CANCELLED);
    }

    public boolean markFailed() {
        while (true) {
            RestoreJobStatus currentStatus = status.get();
            if (currentStatus == RestoreJobStatus.FAILED) {
                return true;
            }

            if (currentStatus == RestoreJobStatus.COMPLETED
                    || currentStatus == RestoreJobStatus.CANCELLED) {
                return false;
            }

            if (status.compareAndSet(currentStatus, RestoreJobStatus.FAILED)) {
                Instant now = Instant.now();
                completedAt.compareAndSet(null, now);
                updatedAt.set(now);
                return true;
            }
        }
    }

    public void recordExecutionResult(RestoreExecutionResult result) {
        executionResult.set(result);
        updatedAt.set(Instant.now());
    }

    public void recordFailure(Throwable failure) {
        errorMessage.set(failure.getMessage());
        updatedAt.set(Instant.now());
    }

    private RestoreJobStateException invalidTransition(
            RestoreJobStatus currentStatus,
            RestoreJobStatus targetStatus
    ) {
        return new RestoreJobStateException(
                "Restore job cannot transition from "
                        + currentStatus
                        + " to "
                        + targetStatus
                        + ". jobId="
                        + jobId
        );
    }
}
