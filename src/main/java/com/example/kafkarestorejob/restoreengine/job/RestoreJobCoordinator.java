package com.example.kafkarestorejob.restoreengine.job;

import com.example.kafkarestorejob.restoreengine.config.EngineKafkaProperties;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreExecutionResult;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreReplicationLoop;
import com.example.kafkarestorejob.restoreengine.zookeeper.ZooKeeperCommandProcessorStateRepository;
import jakarta.persistence.EntityNotFoundException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class RestoreJobCoordinator {

    private final RestoreJobRepository restoreJobRepository;
    private final RestoreReplicationLoop restoreReplicationLoop;
    private final ZooKeeperCommandProcessorStateRepository stateRepository;
    private final EngineKafkaProperties engineKafkaProperties;
    private final TransactionTemplate transactionTemplate;
    private final ThreadPoolTaskExecutor restoreJobExecutor;
    private final Map<UUID, RunningRestoreJob> runningJobs = new ConcurrentHashMap<>();
    private final Object activeJobLock = new Object();

    public RestoreJobCoordinator(
            RestoreJobRepository restoreJobRepository,
            RestoreReplicationLoop restoreReplicationLoop,
            ZooKeeperCommandProcessorStateRepository stateRepository,
            EngineKafkaProperties engineKafkaProperties,
            TransactionTemplate transactionTemplate,
            @Qualifier("restoreJobExecutor") ThreadPoolTaskExecutor restoreJobExecutor
    ) {
        this.restoreJobRepository = restoreJobRepository;
        this.restoreReplicationLoop = restoreReplicationLoop;
        this.stateRepository = stateRepository;
        this.engineKafkaProperties = engineKafkaProperties;
        this.transactionTemplate = transactionTemplate;
        this.restoreJobExecutor = restoreJobExecutor;
    }

    public RestoreJobStartResponse startJob(String restoreType, Instant restoreFromTimestamp) {
        engineKafkaProperties.requirePipeline(restoreType);
        synchronized (activeJobLock) {
            if (!runningJobs.isEmpty()) {
                throw new IllegalStateException("Another restore job is already active");
            }
            RestoreJobEntity saved = Objects.requireNonNull(
                    transactionTemplate.execute(status -> createPendingJob(restoreType, restoreFromTimestamp)),
                    "Created restore job must not be null"
            );
            RunningRestoreJob runningRestoreJob = new RunningRestoreJob();
            runningJobs.put(saved.getId(), runningRestoreJob);
            try {
                restoreJobExecutor.submit(() -> executeJob(saved.getId(), restoreType, restoreFromTimestamp, runningRestoreJob));
            } catch (RejectedExecutionException exception) {
                runningJobs.remove(saved.getId());
                markFailed(saved.getId(), exception);
                throw exception;
            }
            return new RestoreJobStartResponse(saved.getId());
        }
    }

    @Transactional(readOnly = true)
    public RestoreJobResponse getJob(UUID jobId) {
        RestoreJobEntity entity = findJob(jobId);
        return RestoreJobResponse.fromEntity(entity, runningJobs.get(jobId));
    }

    @Transactional(readOnly = true)
    public List<RestoreJobResponse> listJobs() {
        return restoreJobRepository.findTop50ByOrderByRequestedAtDesc().stream()
                .map(entity -> RestoreJobResponse.fromEntity(entity, runningJobs.get(entity.getId())))
                .toList();
    }

    @Transactional
    public RestoreJobResponse requestCancellation(UUID jobId) {
        RunningRestoreJob runningRestoreJob = runningJobs.get(jobId);
        if (runningRestoreJob == null) {
            return RestoreJobResponse.fromEntity(findJob(jobId));
        }

        RestoreJobEntity entity = findJob(jobId);
        if (!runningRestoreJob.requestCancellation()) {
            return RestoreJobResponse.fromEntity(entity, runningRestoreJob);
        }
        entity.setStatus(runningRestoreJob.getStatus());
        entity.setCancellationRequestedAt(runningRestoreJob.getCancellationRequestedAt());
        return RestoreJobResponse.fromEntity(restoreJobRepository.save(entity));
    }

    private void executeJob(UUID jobId, String restoreType, Instant restoreFromTimestamp, RunningRestoreJob runningRestoreJob) {
        runningRestoreJob.setStatus(RestoreJobStatus.RUNNING);
        markRunning(jobId, restoreType);
        try {
            RestoreExecutionResult result = restoreReplicationLoop.restore(
                    restoreType,
                    restoreFromTimestamp,
                    runningRestoreJob::isCancellationRequested
            );
            runningRestoreJob.setStatus(RestoreJobStatus.FINALIZING);
            markFinalizing(jobId);
            stateRepository.markRestoreCompleted(result);
            runningRestoreJob.setStatus(RestoreJobStatus.COMPLETED);
            markCompleted(jobId, result);
        } catch (RestoreJobCancellationException exception) {
            runningRestoreJob.setStatus(RestoreJobStatus.CANCELLED);
            markCancelled(jobId, exception.getMessage());
        } catch (RuntimeException exception) {
            runningRestoreJob.setStatus(RestoreJobStatus.FAILED);
            markFailed(jobId, exception);
        } finally {
            runningJobs.remove(jobId);
        }
    }

    private RestoreJobEntity createPendingJob(String restoreType, Instant restoreFromTimestamp) {
        RestoreJobEntity entity = new RestoreJobEntity();
        entity.setId(UUID.randomUUID());
        entity.setRestoreType(restoreType);
        entity.setStatus(RestoreJobStatus.PENDING);
        entity.setRequestedAt(Instant.now());
        entity.setRestoreFromTimestamp(restoreFromTimestamp);
        return restoreJobRepository.save(entity);
    }

    private void markRunning(UUID jobId, String restoreType) {
        transactionTemplate.executeWithoutResult(status -> updateJobAsRunning(jobId, restoreType));
    }

    private void updateJobAsRunning(UUID jobId, String restoreType) {
        RestoreJobEntity entity = findJob(jobId);
        EngineKafkaProperties.PipelineProperties pipeline = engineKafkaProperties.requirePipeline(restoreType);
        entity.setStatus(RestoreJobStatus.RUNNING);
        entity.setStartedAt(Instant.now());
        entity.setSourceTopic(pipeline.getSourceTopic());
        entity.setTargetTopic(pipeline.getTargetTopic());
        entity.setMessageType(pipeline.getMessageType());
        restoreJobRepository.save(entity);
    }

    private void markFinalizing(UUID jobId) {
        transactionTemplate.executeWithoutResult(status -> updateJobAsFinalizing(jobId));
    }

    private void updateJobAsFinalizing(UUID jobId) {
        RestoreJobEntity entity = findJob(jobId);
        entity.setStatus(RestoreJobStatus.FINALIZING);
        restoreJobRepository.save(entity);
    }

    private void markCompleted(UUID jobId, RestoreExecutionResult result) {
        transactionTemplate.executeWithoutResult(status -> updateJobAsCompleted(jobId, result));
    }

    private void updateJobAsCompleted(UUID jobId, RestoreExecutionResult result) {
        RestoreJobEntity entity = findJob(jobId);
        entity.setStatus(RestoreJobStatus.COMPLETED);
        entity.setCompletedAt(Instant.now());
        entity.setSourceTopic(result.sourceTopic());
        entity.setTargetTopic(result.targetTopic());
        entity.setMessageType(result.messageType());
        entity.setBatchesCommitted(result.batchesCommitted());
        entity.setRecordsRestored(result.recordsRestored());
        restoreJobRepository.save(entity);
    }

    private void markCancelled(UUID jobId, String reason) {
        transactionTemplate.executeWithoutResult(status -> updateJobAsCancelled(jobId, reason));
    }

    private void updateJobAsCancelled(UUID jobId, String reason) {
        RestoreJobEntity entity = findJob(jobId);
        entity.setStatus(RestoreJobStatus.CANCELLED);
        entity.setCompletedAt(Instant.now());
        entity.setErrorMessage(reason);
        restoreJobRepository.save(entity);
    }

    private void markFailed(UUID jobId, RuntimeException exception) {
        transactionTemplate.executeWithoutResult(status -> updateJobAsFailed(jobId, exception));
    }

    private void updateJobAsFailed(UUID jobId, RuntimeException exception) {
        RestoreJobEntity entity = findJob(jobId);
        entity.setStatus(RestoreJobStatus.FAILED);
        entity.setCompletedAt(Instant.now());
        entity.setErrorMessage(exception.getMessage());
        restoreJobRepository.save(entity);
    }

    private RestoreJobEntity findJob(UUID jobId) {
        return restoreJobRepository.findById(jobId)
                .orElseThrow(() -> new EntityNotFoundException("Restore job not found: " + jobId));
    }
}
