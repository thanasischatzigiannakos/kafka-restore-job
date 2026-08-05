package com.example.kafkarestorejob.restoreengine.job;

/**
 * Enumerates the lifecycle states of a restore job.
 */
public enum RestoreJobStatus {
    PENDING,
    RUNNING,
    CANCELLATION_REQUESTED,
    FINALIZING,
    CANCELLED,
    COMPLETED,
    FAILED
}
