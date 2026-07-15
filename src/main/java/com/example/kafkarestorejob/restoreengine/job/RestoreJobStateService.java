package com.example.kafkarestorejob.restoreengine.job;

import com.example.kafkarestorejob.restoreengine.kafka.RestoreExecutionResult;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface RestoreJobStateService {

    RestoreJobEntity createPending(UUID jobId, String restoreType, Instant restoreFromTimestamp);

    void persistRunning(UUID jobId, String restoreType);

    boolean persistCancellationRequested(UUID jobId, Instant cancellationRequestedAt);

    void persistFinalizing(UUID jobId);

    void persistExecutionResult(UUID jobId, RestoreExecutionResult result);

    void persistCompleted(UUID jobId);

    void persistCancelled(UUID jobId);

    void persistFailed(UUID jobId, Throwable failure);

    RestoreJobEntity findJob(UUID jobId);

    List<RestoreJobEntity> listJobs();
}
