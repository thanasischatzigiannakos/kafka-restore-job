package com.example.kafkarestorejob.restoreengine.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.kafkarestorejob.restoreengine.config.EngineKafkaProperties;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreExecutionResult;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
class RestoreJobCoordinatorTest {

    @Mock
    private RestoreReplicationLoop restoreReplicationLoop;

    @Mock
    private EngineKafkaProperties engineKafkaProperties;

    @Mock
    private ThreadPoolTaskExecutor restoreJobExecutor;

    private RestoreJobCoordinator coordinator;
    private EngineKafkaProperties.PipelineProperties pipelineProperties;

    @BeforeEach
    void setUp() {
        coordinator = new RestoreJobCoordinator(
                restoreReplicationLoop,
                engineKafkaProperties,
                restoreJobExecutor
        );
        pipelineProperties = new EngineKafkaProperties.PipelineProperties();
        pipelineProperties.setMessageType("application");
        pipelineProperties.setSourceTopic("source-topic");
        pipelineProperties.setTargetTopic("target-topic");
    }

    @Test
    void marksRunningBeforeInvokingLoop() {
        UUID jobId = UUID.randomUUID();
        InMemoryRestoreJob job = newJob(jobId);
        RestoreJobExecutionContext context = job.getContext();
        addRunningJob(jobId, job);
        RestoreExecutionResult result = result();
        when(engineKafkaProperties.requirePipeline("application")).thenReturn(pipelineProperties);
        when(restoreReplicationLoop.restore("application", pipelineProperties, null, context)).thenAnswer(invocation -> {
            assertEquals(RestoreJobStatus.RUNNING, context.getStatus());
            context.tryMarkFinalizing();
            return result;
        });

        ReflectionTestUtils.invokeMethod(
                coordinator,
                "executeJob",
                jobId,
                "application",
                null,
                job
        );

        verify(restoreReplicationLoop).restore("application", pipelineProperties, null, context);
    }

    @Test
    void cancellationDelegatesToContextAndReturnsUpdatedSnapshot() {
        UUID jobId = UUID.randomUUID();
        InMemoryRestoreJob job = newJob(jobId);
        RestoreJobExecutionContext context = job.getContext();
        context.markRunning();
        addJob(jobId, job);

        RestoreJobResponse response = coordinator.requestCancellation(jobId);

        assertEquals(RestoreJobStatus.CANCELLATION_REQUESTED, response.status());
    }

    @Test
    void cancellationIsRejectedAfterFinalizing() {
        UUID jobId = UUID.randomUUID();
        InMemoryRestoreJob job = newJob(jobId);
        RestoreJobExecutionContext context = job.getContext();
        context.markRunning();
        context.tryMarkFinalizing();
        addJob(jobId, job);

        assertThrows(IllegalStateException.class, () -> coordinator.requestCancellation(jobId));
    }

    @Test
    void recordsResultBeforeMarkingCompleted() {
        UUID jobId = UUID.randomUUID();
        InMemoryRestoreJob job = newJob(jobId);
        RestoreJobExecutionContext context = job.getContext();
        addRunningJob(jobId, job);
        RestoreExecutionResult result = result();
        when(engineKafkaProperties.requirePipeline("application")).thenReturn(pipelineProperties);
        when(restoreReplicationLoop.restore("application", pipelineProperties, null, context)).thenAnswer(invocation -> {
            context.tryMarkFinalizing();
            assertEquals(RestoreJobStatus.FINALIZING, context.getStatus());
            return result;
        });

        ReflectionTestUtils.invokeMethod(
                coordinator,
                "executeJob",
                jobId,
                "application",
                null,
                job
        );

        assertEquals(result, job.getExecutionResult());
        assertEquals(RestoreJobStatus.COMPLETED, context.getStatus());
    }

    @Test
    void cancellationExceptionProducesCancelled() {
        UUID jobId = UUID.randomUUID();
        InMemoryRestoreJob job = newJob(jobId);
        RestoreJobExecutionContext context = job.getContext();
        addRunningJob(jobId, job);
        when(engineKafkaProperties.requirePipeline("application")).thenReturn(pipelineProperties);
        when(restoreReplicationLoop.restore("application", pipelineProperties, null, context)).thenAnswer(invocation -> {
            context.requestCancellation();
            throw new RestoreJobCancellationException("cancelled");
        });

        ReflectionTestUtils.invokeMethod(
                coordinator,
                "executeJob",
                jobId,
                "application",
                null,
                job
        );

        assertEquals(RestoreJobStatus.CANCELLED, context.getStatus());
    }

    @Test
    void runtimeExceptionProducesFailed() {
        UUID jobId = UUID.randomUUID();
        InMemoryRestoreJob job = newJob(jobId);
        RestoreJobExecutionContext context = job.getContext();
        addRunningJob(jobId, job);
        RuntimeException failure = new RuntimeException("boom");
        when(engineKafkaProperties.requirePipeline("application")).thenReturn(pipelineProperties);
        when(restoreReplicationLoop.restore("application", pipelineProperties, null, context)).thenThrow(failure);

        ReflectionTestUtils.invokeMethod(
                coordinator,
                "executeJob",
                jobId,
                "application",
                null,
                job
        );

        assertEquals(RestoreJobStatus.FAILED, context.getStatus());
        assertEquals("boom", job.getErrorMessage());
    }

    @Test
    void alwaysRemovesJobFromRunningMap() {
        UUID jobId = UUID.randomUUID();
        InMemoryRestoreJob job = newJob(jobId);
        RestoreJobExecutionContext context = job.getContext();
        addRunningJob(jobId, job);
        when(engineKafkaProperties.requirePipeline("application")).thenReturn(pipelineProperties);
        when(restoreReplicationLoop.restore("application", pipelineProperties, null, context))
                .thenThrow(new RuntimeException("boom"));

        ReflectionTestUtils.invokeMethod(
                coordinator,
                "executeJob",
                jobId,
                "application",
                null,
                job
        );

        assertEquals(0, runningJobs().size());
    }

    @SuppressWarnings("unchecked")
    private Map<UUID, RestoreJobExecutionContext> runningJobs() {
        return (Map<UUID, RestoreJobExecutionContext>) ReflectionTestUtils.getField(
                coordinator,
                "runningJobs"
        );
    }

    private void addRunningJob(UUID jobId, InMemoryRestoreJob job) {
        runningJobs().put(jobId, job.getContext());
        jobs().put(jobId, job);
    }

    @SuppressWarnings("unchecked")
    private Map<UUID, InMemoryRestoreJob> jobs() {
        return (Map<UUID, InMemoryRestoreJob>) ReflectionTestUtils.getField(
                coordinator,
                "jobs"
        );
    }

    private void addJob(UUID jobId, InMemoryRestoreJob job) {
        jobs().put(jobId, job);
    }

    private RestoreExecutionResult result() {
        return new RestoreExecutionResult(
                "application",
                "source-topic",
                "target-topic",
                "restore-group",
                "restore-tx",
                "application",
                1,
                1L,
                0
        );
    }

    private InMemoryRestoreJob newJob(UUID jobId) {
        return new InMemoryRestoreJob(
                jobId,
                "application",
                Instant.now(),
                null,
                new RestoreJobExecutionContext(jobId)
        );
    }
}
