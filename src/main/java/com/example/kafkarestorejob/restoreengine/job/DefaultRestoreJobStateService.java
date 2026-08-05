package com.example.kafkarestorejob.restoreengine.job;

import com.example.kafkarestorejob.restoreengine.config.EngineKafkaProperties;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreExecutionResult;
import jakarta.persistence.EntityNotFoundException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Default JPA-backed implementation of the restore job state service.
 */
@Service
public class DefaultRestoreJobStateService implements RestoreJobStateService {

    private final RestoreJobRepository restoreJobRepository;
    private final EngineKafkaProperties engineKafkaProperties;

    /**
     * Creates the service with the repository and Kafka pipeline properties used to enrich job
     * metadata.
     *
     * @param restoreJobRepository the restore job repository
     * @param engineKafkaProperties the Kafka pipeline properties
     */
    public DefaultRestoreJobStateService(
            RestoreJobRepository restoreJobRepository,
            EngineKafkaProperties engineKafkaProperties
    ) {
        this.restoreJobRepository = restoreJobRepository;
        this.engineKafkaProperties = engineKafkaProperties;
    }

    @Override
    @Transactional
    /**
     * Persists a newly created restore job in the pending state.
     *
     * @param jobId the restore job identifier
     * @param restoreType the logical restore type
     * @param restoreFromTimestamp the optional timestamp used to position the source consumer
     * @return the persisted entity
     */
    public RestoreJobEntity createPending(UUID jobId, String restoreType, Instant restoreFromTimestamp) {
        RestoreJobEntity entity = new RestoreJobEntity();
        entity.setId(jobId);
        entity.setRestoreType(restoreType);
        entity.setStatus(RestoreJobStatus.PENDING);
        entity.setRequestedAt(Instant.now());
        entity.setRestoreFromTimestamp(restoreFromTimestamp);
        return restoreJobRepository.save(entity);
    }

    @Override
    @Transactional
    /**
     * Persists the transition of a restore job into the running state and captures the resolved
     * pipeline topics.
     *
     * @param jobId the restore job identifier
     * @param restoreType the logical restore type
     */
    public void persistRunning(UUID jobId, String restoreType) {
        RestoreJobEntity entity = findJob(jobId);
        EngineKafkaProperties.PipelineProperties pipeline = engineKafkaProperties.requirePipeline(restoreType);
        entity.setStatus(RestoreJobStatus.RUNNING);
        entity.setStartedAt(Instant.now());
        entity.setSourceTopic(pipeline.getSourceTopic());
        entity.setTargetTopic(pipeline.getTargetTopic());
        entity.setMessageType(restoreType);
        restoreJobRepository.save(entity);
    }

    @Override
    @Transactional
    /**
     * Persists a cancellation request when the current restore job state allows it.
     *
     * @param jobId the restore job identifier
     * @param cancellationRequestedAt the cancellation timestamp
     * @return {@code true} when the cancellation request was persisted
     */
    public boolean persistCancellationRequested(UUID jobId, Instant cancellationRequestedAt) {
        return restoreJobRepository.updateCancellationRequested(
                jobId,
                RestoreJobStatus.CANCELLATION_REQUESTED,
                cancellationRequestedAt,
                List.of(RestoreJobStatus.PENDING, RestoreJobStatus.RUNNING)
        ) == 1;
    }

    @Override
    @Transactional
    /**
     * Persists the transition to the finalizing state.
     *
     * @param jobId the restore job identifier
     */
    public void persistFinalizing(UUID jobId) {
        int updatedRows = restoreJobRepository.updateStatusIfCurrent(
                jobId,
                RestoreJobStatus.FINALIZING,
                RestoreJobStatus.RUNNING
        );
        if (updatedRows != 1) {
            throw new RestoreJobStateException(
                    "Restore job could not be persisted as FINALIZING. jobId=" + jobId
            );
        }
    }

    @Override
    @Transactional
    /**
     * Persists the execution result produced by the restore loop.
     *
     * @param jobId the restore job identifier
     * @param result the execution result
     */
    public void persistExecutionResult(UUID jobId, RestoreExecutionResult result) {
        RestoreJobEntity entity = findJob(jobId);
        entity.setSourceTopic(result.sourceTopic());
        entity.setTargetTopic(result.targetTopic());
        entity.setMessageType(result.messageType());
        entity.setCommittedTransactions(result.transactionsCommitted());
        entity.setRecordsRestored(result.recordsRestored());
        restoreJobRepository.save(entity);
    }

    @Override
    @Transactional
    /**
     * Persists the transition to the completed state.
     *
     * @param jobId the restore job identifier
     */
    public void persistCompleted(UUID jobId) {
        int updatedRows = restoreJobRepository.updateCompleted(
                jobId,
                RestoreJobStatus.COMPLETED,
                Instant.now(),
                RestoreJobStatus.FINALIZING
        );
        if (updatedRows != 1) {
            throw new RestoreJobStateException(
                    "Restore job could not be persisted as COMPLETED. jobId=" + jobId
            );
        }
    }

    @Override
    @Transactional
    /**
     * Persists the transition to the cancelled state.
     *
     * @param jobId the restore job identifier
     */
    public void persistCancelled(UUID jobId) {
        RestoreJobEntity entity = findJob(jobId);
        entity.setStatus(RestoreJobStatus.CANCELLED);
        entity.setCompletedAt(Instant.now());
        restoreJobRepository.save(entity);
    }

    @Override
    @Transactional
    /**
     * Persists a terminal failure for a restore job.
     *
     * @param jobId the restore job identifier
     * @param failure the failure that terminated the job
     */
    public void persistFailed(UUID jobId, Throwable failure) {
        RestoreJobEntity entity = findJob(jobId);
        entity.setStatus(RestoreJobStatus.FAILED);
        entity.setCompletedAt(Instant.now());
        entity.setErrorMessage(failure.getMessage());
        restoreJobRepository.save(entity);
    }

    @Override
    @Transactional(readOnly = true)
    /**
     * Loads one persisted restore job.
     *
     * @param jobId the restore job identifier
     * @return the persisted restore job entity
     */
    public RestoreJobEntity findJob(UUID jobId) {
        return restoreJobRepository.findById(jobId)
                .orElseThrow(() -> new EntityNotFoundException("Restore job not found: " + jobId));
    }

    @Override
    @Transactional(readOnly = true)
    /**
     * Lists the most recent persisted restore jobs.
     *
     * @return the persisted restore job entities
     */
    public List<RestoreJobEntity> listJobs() {
        return restoreJobRepository.findTop50ByOrderByRequestedAtDesc();
    }
}
