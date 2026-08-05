package com.example.kafkarestorejob.restoreengine.job;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;

/**
 * Thread-safe in-memory execution context tracking the lifecycle of one running restore job.
 */
public final class RestoreJobExecutionContext {

    @Getter
    private final UUID jobId;
    private final AtomicReference<RestoreJobStatus> status =
            new AtomicReference<>(RestoreJobStatus.PENDING);
    private final AtomicReference<Instant> cancellationRequestedAt = new AtomicReference<>();

    /**
     * Creates the execution context for one restore job.
     *
     * @param jobId the restore job identifier
     */
    public RestoreJobExecutionContext(UUID jobId) {
        this.jobId = jobId;
    }

    /**
     * Returns the current in-memory status of the restore job.
     *
     * @return the current status
     */
    public RestoreJobStatus getStatus() {
        return status.get();
    }

    /**
     * Returns the timestamp at which cancellation was requested, when present.
     *
     * @return the cancellation-request timestamp
     */
    public Instant getCancellationRequestedAt() {
        return cancellationRequestedAt.get();
    }

    /**
     * Transitions the job from pending to running or fails when another state already won.
     */
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

    /**
     * Requests cancellation of the restore job when the current state allows it.
     *
     * @return {@code true} when cancellation is now requested or was already requested
     */
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

    /**
     * Returns whether cancellation has been requested or the job has already been cancelled.
     *
     * @return {@code true} when cancellation is in effect
     */
    public boolean isCancellationRequested() {
        RestoreJobStatus currentStatus = status.get();
        return currentStatus == RestoreJobStatus.CANCELLATION_REQUESTED
                || currentStatus == RestoreJobStatus.CANCELLED;
    }

    /**
     * Throws a cancellation exception when the job has been marked for cancellation.
     */
    public void throwIfCancellationRequested() {
        if (isCancellationRequested()) {
            throw new RestoreJobCancellationException(
                    "Cancellation requested for running restore job. jobId=" + jobId
            );
        }
    }

    /**
     * Attempts to transition the job from running to finalizing.
     *
     * @return {@code true} when the transition succeeded
     */
    public boolean tryMarkFinalizing() {
        return status.compareAndSet(RestoreJobStatus.RUNNING, RestoreJobStatus.FINALIZING);
    }

    /**
     * Transitions the job from finalizing to completed.
     */
    public void markCompleted() {
        if (status.compareAndSet(RestoreJobStatus.FINALIZING, RestoreJobStatus.COMPLETED)) {
            return;
        }

        throw invalidTransition(status.get(), RestoreJobStatus.COMPLETED);
    }

    /**
     * Transitions the job from cancellation requested to cancelled.
     */
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

    /**
     * Attempts to mark the job as failed unless it has already reached a terminal non-failed
     * state.
     *
     * @return {@code true} when the failure state was recorded
     */
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

    /**
     * Creates the invalid-transition exception used when lifecycle state changes are not allowed.
     *
     * @param currentStatus the current job status
     * @param targetStatus the attempted target status
     * @return the invalid-transition exception
     */
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
