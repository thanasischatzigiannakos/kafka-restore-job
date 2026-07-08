package com.example.kafkarestorejob.restoreengine.job;

public enum RestoreJobStatus {
    PENDING,
    RUNNING,
    CANCELLATION_REQUESTED,
    CANCELLED,
    COMPLETED,
    FAILED
}
