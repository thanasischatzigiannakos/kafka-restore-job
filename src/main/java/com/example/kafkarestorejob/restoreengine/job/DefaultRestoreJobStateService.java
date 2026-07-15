package com.example.kafkarestorejob.restoreengine.job;

import com.example.kafkarestorejob.restoreengine.config.EngineKafkaProperties;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreExecutionResult;
import jakarta.persistence.EntityNotFoundException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DefaultRestoreJobStateService implements RestoreJobStateService {

    private final RestoreJobRepository restoreJobRepository;
    private final EngineKafkaProperties engineKafkaProperties;

    public DefaultRestoreJobStateService(
            RestoreJobRepository restoreJobRepository,
            EngineKafkaProperties engineKafkaProperties
    ) {
        this.restoreJobRepository = restoreJobRepository;
        this.engineKafkaProperties = engineKafkaProperties;
    }

    @Override
    @Transactional
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
    public void persistRunning(UUID jobId, String restoreType) {
        RestoreJobEntity entity = findJob(jobId);
        EngineKafkaProperties.PipelineProperties pipeline = engineKafkaProperties.requirePipeline(restoreType);
        entity.setStatus(RestoreJobStatus.RUNNING);
        entity.setStartedAt(Instant.now());
        entity.setSourceTopic(pipeline.getSourceTopic());
        entity.setTargetTopic(pipeline.getTargetTopic());
        entity.setMessageType(pipeline.getMessageType());
        restoreJobRepository.save(entity);
    }

    @Override
    @Transactional
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
    public void persistExecutionResult(UUID jobId, RestoreExecutionResult result) {
        RestoreJobEntity entity = findJob(jobId);
        entity.setSourceTopic(result.sourceTopic());
        entity.setTargetTopic(result.targetTopic());
        entity.setMessageType(result.messageType());
        entity.setBatchesCommitted(result.batchesCommitted());
        entity.setRecordsRestored(result.recordsRestored());
        restoreJobRepository.save(entity);
    }

    @Override
    @Transactional
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
    public void persistCancelled(UUID jobId) {
        RestoreJobEntity entity = findJob(jobId);
        entity.setStatus(RestoreJobStatus.CANCELLED);
        entity.setCompletedAt(Instant.now());
        restoreJobRepository.save(entity);
    }

    @Override
    @Transactional
    public void persistFailed(UUID jobId, Throwable failure) {
        RestoreJobEntity entity = findJob(jobId);
        entity.setStatus(RestoreJobStatus.FAILED);
        entity.setCompletedAt(Instant.now());
        entity.setErrorMessage(failure.getMessage());
        restoreJobRepository.save(entity);
    }

    @Override
    @Transactional(readOnly = true)
    public RestoreJobEntity findJob(UUID jobId) {
        return restoreJobRepository.findById(jobId)
                .orElseThrow(() -> new EntityNotFoundException("Restore job not found: " + jobId));
    }

    @Override
    @Transactional(readOnly = true)
    public List<RestoreJobEntity> listJobs() {
        return restoreJobRepository.findTop50ByOrderByRequestedAtDesc();
    }
}
