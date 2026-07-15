package com.example.kafkarestorejob.restoreengine.job;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;

public final class RestoreJobExecutionContext {

    @Getter
    private final UUID jobId;
    private final AtomicReference<RestoreJobStatus> status =
            new AtomicReference<>(RestoreJobStatus.PENDING);
    private final AtomicReference<Instant> cancellationRequestedAt = new AtomicReference<>();

    public RestoreJobExecutionContext(UUID jobId) {
        this.jobId = jobId;
    }

    public RestoreJobStatus getStatus() {
        return status.get();
    }

    public Instant getCancellationRequestedAt() {
        return cancellationRequestedAt.get();
    }

    public void markRunning() {
        if (status.compareAndSet(RestoreJobStatus.PENDING, RestoreJobStatus.RUNNING)) {
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
                cancellationRequestedAt.compareAndSet(null, Instant.now());
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
        return status.compareAndSet(RestoreJobStatus.RUNNING, RestoreJobStatus.FINALIZING);
    }

    public void markCompleted() {
        if (status.compareAndSet(RestoreJobStatus.FINALIZING, RestoreJobStatus.COMPLETED)) {
            return;
        }

        throw invalidTransition(status.get(), RestoreJobStatus.COMPLETED);
    }

    public void markCancelled() {
        if (status.compareAndSet(
                RestoreJobStatus.CANCELLATION_REQUESTED,
                RestoreJobStatus.CANCELLED)) {
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
                return true;
            }
        }
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
