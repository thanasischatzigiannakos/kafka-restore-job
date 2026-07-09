package com.example.kafkarestorejob.restoreengine.job;

import com.example.kafkarestorejob.restoreengine.config.EngineKafkaProperties;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreExecutionResult;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreReplicationLoop;
import com.example.kafkarestorejob.restoreengine.zookeeper.ZooKeeperCommandProcessorStateRepository;
import jakarta.persistence.EntityNotFoundException;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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

    private static final Set<RestoreJobStatus> ACTIVE_STATUSES =
            Set.copyOf(EnumSet.of(RestoreJobStatus.PENDING, RestoreJobStatus.RUNNING, RestoreJobStatus.CANCELLATION_REQUESTED));

    private final RestoreJobRepository restoreJobRepository;
    private final RestoreReplicationLoop restoreReplicationLoop;
    private final ZooKeeperCommandProcessorStateRepository stateRepository;
    private final EngineKafkaProperties engineKafkaProperties;
    private final TransactionTemplate transactionTemplate;
    private final ThreadPoolTaskExecutor restoreJobExecutor;
    private final Map<UUID, RunningRestoreJob> runningJobs = new ConcurrentHashMap<>();
    private final Map<String, Object> restoreTypeLocks = new ConcurrentHashMap<>();

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

    public RestoreJobResponse startJob(String restoreType) {
        engineKafkaProperties.requirePipeline(restoreType);
        Object restoreTypeLock = restoreTypeLocks.computeIfAbsent(restoreType, ignored -> new Object());
        synchronized (restoreTypeLock) {
            RestoreJobEntity saved = Objects.requireNonNull(
                    transactionTemplate.execute(status -> createPendingJob(restoreType)),
                    "Created restore job must not be null"
            );
            RunningRestoreJob runningRestoreJob = new RunningRestoreJob();
            runningJobs.put(saved.getId(), runningRestoreJob);
            try {
                restoreJobExecutor.submit(() -> executeJob(saved.getId(), restoreType, runningRestoreJob));
            } catch (RejectedExecutionException exception) {
                runningJobs.remove(saved.getId());
                markFailed(saved.getId(), exception);
                throw exception;
            }
            return RestoreJobResponse.fromEntity(saved);
        }
    }

    @Transactional(readOnly = true)
    public RestoreJobResponse getJob(UUID jobId) {
        return RestoreJobResponse.fromEntity(findJob(jobId));
    }

    @Transactional(readOnly = true)
    public List<RestoreJobResponse> listJobs() {
        return restoreJobRepository.findTop50ByOrderByRequestedAtDesc().stream()
                .map(RestoreJobResponse::fromEntity)
                .toList();
    }

    @Transactional
    public RestoreJobResponse requestCancellation(UUID jobId) {
        RestoreJobEntity entity = findJob(jobId);
        if (entity.getStatus() == RestoreJobStatus.COMPLETED
                || entity.getStatus() == RestoreJobStatus.CANCELLED
                || entity.getStatus() == RestoreJobStatus.FAILED) {
            return RestoreJobResponse.fromEntity(entity);
        }

        entity.setStatus(RestoreJobStatus.CANCELLATION_REQUESTED);
        entity.setCancellationRequestedAt(Instant.now());
        RunningRestoreJob runningRestoreJob = runningJobs.get(jobId);
        if (runningRestoreJob != null) {
            runningRestoreJob.requestCancellation();
        }

        return RestoreJobResponse.fromEntity(restoreJobRepository.save(entity));
    }

    private void executeJob(UUID jobId, String restoreType, RunningRestoreJob runningRestoreJob) {
        markRunning(jobId, restoreType);
        try {
            RestoreExecutionResult result = restoreReplicationLoop.restore(restoreType, runningRestoreJob::isCancellationRequested);
            markCompleted(jobId, result);
            stateRepository.markRestoreCompleted(result);
        } catch (RestoreJobCancellationException exception) {
            markCancelled(jobId, exception.getMessage());
        } catch (RuntimeException exception) {
            markFailed(jobId, exception);
        } finally {
            runningJobs.remove(jobId);
        }
    }

    private RestoreJobEntity createPendingJob(String restoreType) {
        if (restoreJobRepository.existsByRestoreTypeAndStatusIn(restoreType, ACTIVE_STATUSES)) {
            throw new IllegalStateException("Another restore job is already active for restore type: " + restoreType);
        }

        RestoreJobEntity entity = new RestoreJobEntity();
        entity.setId(UUID.randomUUID());
        entity.setRestoreType(restoreType);
        entity.setStatus(RestoreJobStatus.PENDING);
        entity.setRequestedAt(Instant.now());
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
