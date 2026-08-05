package com.example.kafkarestorejob.restoreengine.job;

import com.example.kafkarestorejob.restoreengine.kafka.RestoreExecutionResult;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Persists the lifecycle state of restore jobs.
 */
public interface RestoreJobStateService {

    /**
     * Creates a new pending restore job record.
     *
     * @param jobId the restore job identifier
     * @param restoreType the logical restore type
     * @param restoreFromTimestamp the optional timestamp used to position the source consumer
     * @return the persisted restore job entity
     */
    RestoreJobEntity createPending(UUID jobId, String restoreType, Instant restoreFromTimestamp);

    /**
     * Persists the transition to the running state.
     *
     * @param jobId the restore job identifier
     * @param restoreType the logical restore type
     */
    void persistRunning(UUID jobId, String restoreType);

    /**
     * Persists a cancellation request when the current state allows it.
     *
     * @param jobId the restore job identifier
     * @param cancellationRequestedAt the cancellation timestamp
     * @return {@code true} when the cancellation request was persisted
     */
    boolean persistCancellationRequested(UUID jobId, Instant cancellationRequestedAt);

    /**
     * Persists the transition to the finalizing state.
     *
     * @param jobId the restore job identifier
     */
    void persistFinalizing(UUID jobId);

    /**
     * Persists the execution result produced by the restore loop.
     *
     * @param jobId the restore job identifier
     * @param result the execution result
     */
    void persistExecutionResult(UUID jobId, RestoreExecutionResult result);

    /**
     * Persists the transition to the completed state.
     *
     * @param jobId the restore job identifier
     */
    void persistCompleted(UUID jobId);

    /**
     * Persists the transition to the cancelled state.
     *
     * @param jobId the restore job identifier
     */
    void persistCancelled(UUID jobId);

    /**
     * Persists a terminal failure for the restore job.
     *
     * @param jobId the restore job identifier
     * @param failure the failure that terminated the job
     */
    void persistFailed(UUID jobId, Throwable failure);

    /**
     * Returns one persisted restore job.
     *
     * @param jobId the restore job identifier
     * @return the persisted job entity
     */
    RestoreJobEntity findJob(UUID jobId);

    /**
     * Lists persisted restore jobs in repository-defined order.
     *
     * @return the persisted restore job entities
     */
    List<RestoreJobEntity> listJobs();
}
