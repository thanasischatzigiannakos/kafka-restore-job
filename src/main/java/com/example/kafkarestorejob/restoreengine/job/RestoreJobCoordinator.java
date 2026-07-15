package com.example.kafkarestorejob.restoreengine.job;

import com.example.kafkarestorejob.restoreengine.config.EngineKafkaProperties;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreExecutionResult;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

@Service
public class RestoreJobCoordinator {

    private static final Logger log = LoggerFactory.getLogger(RestoreJobCoordinator.class);

    private final RestoreReplicationLoop restoreReplicationLoop;
    private final EngineKafkaProperties engineKafkaProperties;
    private final ThreadPoolTaskExecutor restoreJobExecutor;
    private final Map<UUID, InMemoryRestoreJob> jobs = new ConcurrentHashMap<>();
    private final Map<UUID, RestoreJobExecutionContext> runningJobs = new ConcurrentHashMap<>();
    private final Object activeJobLock = new Object();

    public RestoreJobCoordinator(
            RestoreReplicationLoop restoreReplicationLoop,
            EngineKafkaProperties engineKafkaProperties,
            @Qualifier("restoreJobExecutor") ThreadPoolTaskExecutor restoreJobExecutor
    ) {
        this.restoreReplicationLoop = restoreReplicationLoop;
        this.engineKafkaProperties = engineKafkaProperties;
        this.restoreJobExecutor = restoreJobExecutor;
    }

    public RestoreJobStartResponse startJob(String restoreType, Instant restoreFromTimestamp) {
        UUID jobId = UUID.randomUUID();
        engineKafkaProperties.requirePipeline(restoreType);

        synchronized (activeJobLock) {
            if (!runningJobs.isEmpty()) {
                throw new IllegalStateException("Another restore job is already active");
            }

            Instant requestedAt = Instant.now();
            RestoreJobExecutionContext context = new RestoreJobExecutionContext(jobId);
            InMemoryRestoreJob job = new InMemoryRestoreJob(
                    jobId,
                    restoreType,
                    requestedAt,
                    restoreFromTimestamp,
                    context
            );
            jobs.put(jobId, job);
            runningJobs.put(jobId, context);
            try {
                restoreJobExecutor.submit(
                        () -> executeJob(jobId, restoreType, restoreFromTimestamp, job));
            } catch (RejectedExecutionException exception) {
                runningJobs.remove(jobId);
                job.recordFailure(exception);
                context.markFailed();
                throw exception;
            } catch (RuntimeException exception) {
                runningJobs.remove(jobId);
                jobs.remove(jobId);
                throw exception;
            }

            log.info("Created restore job {} in PENDING state", jobId);
            return new RestoreJobStartResponse(jobId);
        }
    }

    public RestoreJobResponse getJob(UUID jobId) {
        InMemoryRestoreJob job = jobs.get(jobId);
        if (job == null) {
            throw new IllegalStateException("Restore job not found: " + jobId);
        }
        return RestoreJobResponse.fromInMemoryJob(job);
    }

    public List<RestoreJobResponse> listJobs() {
        return jobs.values().stream()
                .map(RestoreJobResponse::fromInMemoryJob)
                .sorted((left, right) -> right.requestedAt().compareTo(left.requestedAt()))
                .toList();
    }

    public RestoreJobResponse requestCancellation(UUID jobId) {
        InMemoryRestoreJob job = jobs.get(jobId);
        if (job == null) {
            throw new IllegalStateException("Restore job not found: " + jobId);
        }
        RestoreJobExecutionContext context = job.getContext();

        if (!context.requestCancellation()) {
            throw cancellationRejected(jobId, context.getStatus());
        }

        log.info("Accepted cancellation request for restore job {}", jobId);
        return RestoreJobResponse.fromInMemoryJob(job);
    }

    private void executeJob(
            UUID jobId,
            String restoreType,
            Instant restoreFromTimestamp,
            InMemoryRestoreJob job
    ) {
        RestoreJobExecutionContext context = job.getContext();
        try {
            context.markRunning();
            job.markStarted();
            log.info("Restore job {} entered RUNNING", jobId);

            RestoreExecutionResult result =
                    restoreReplicationLoop.restore(restoreType, restoreFromTimestamp, context);

            job.recordExecutionResult(result);
            context.markCompleted();
            job.markCompleted();
            log.info("Restore job {} entered COMPLETED", jobId);
        } catch (RestoreJobCancellationException exception) {
            context.markCancelled();
            job.markCancelled();
            log.info("Restore job {} entered CANCELLED", jobId);
        } catch (RuntimeException exception) {
            if (context.markFailed()) {
                job.recordFailure(exception);
            }
            log.error("Restore job {} entered FAILED", jobId, exception);
        } finally {
            runningJobs.remove(jobId);
        }
    }

    private IllegalStateException cancellationRejected(UUID jobId, RestoreJobStatus status) {
        return new IllegalStateException(
                "Cancellation rejected for restore job "
                        + jobId
                        + " with status "
                        + status
        );
    }
}
