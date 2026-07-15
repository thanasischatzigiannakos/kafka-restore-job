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

    @BeforeEach
    void setUp() {
        coordinator = new RestoreJobCoordinator(
                restoreReplicationLoop,
                engineKafkaProperties,
                restoreJobExecutor
        );
    }

    @Test
    void marksRunningBeforeInvokingLoop() {
        UUID jobId = UUID.randomUUID();
        RestoreJobExecutionContext context = newContext(jobId);
        addRunningJob(jobId, context);
        RestoreExecutionResult result = result();
        when(restoreReplicationLoop.restore("application", null, context)).thenAnswer(invocation -> {
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
                context
        );

        verify(restoreReplicationLoop).restore("application", null, context);
    }

    @Test
    void cancellationDelegatesToContextAndReturnsUpdatedSnapshot() {
        UUID jobId = UUID.randomUUID();
        RestoreJobExecutionContext context = newContext(jobId);
        context.markRunning();
        addJob(jobId, context);

        RestoreJobResponse response = coordinator.requestCancellation(jobId);

        assertEquals(RestoreJobStatus.CANCELLATION_REQUESTED, response.status());
    }

    @Test
    void cancellationIsRejectedAfterFinalizing() {
        UUID jobId = UUID.randomUUID();
        RestoreJobExecutionContext context = newContext(jobId);
        context.markRunning();
        context.tryMarkFinalizing();
        addJob(jobId, context);

        assertThrows(IllegalStateException.class, () -> coordinator.requestCancellation(jobId));
    }

    @Test
    void recordsResultBeforeMarkingCompleted() {
        UUID jobId = UUID.randomUUID();
        RestoreJobExecutionContext context = newContext(jobId);
        addRunningJob(jobId, context);
        RestoreExecutionResult result = result();
        when(restoreReplicationLoop.restore("application", null, context)).thenAnswer(invocation -> {
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
                context
        );

        assertEquals(result, context.getExecutionResult());
        assertEquals(RestoreJobStatus.COMPLETED, context.getStatus());
    }

    @Test
    void cancellationExceptionProducesCancelled() {
        UUID jobId = UUID.randomUUID();
        RestoreJobExecutionContext context = newContext(jobId);
        addRunningJob(jobId, context);
        when(restoreReplicationLoop.restore("application", null, context)).thenAnswer(invocation -> {
            context.requestCancellation();
            throw new RestoreJobCancellationException("cancelled");
        });

        ReflectionTestUtils.invokeMethod(
                coordinator,
                "executeJob",
                jobId,
                "application",
                null,
                context
        );

        assertEquals(RestoreJobStatus.CANCELLED, context.getStatus());
    }

    @Test
    void runtimeExceptionProducesFailed() {
        UUID jobId = UUID.randomUUID();
        RestoreJobExecutionContext context = newContext(jobId);
        addRunningJob(jobId, context);
        RuntimeException failure = new RuntimeException("boom");
        when(restoreReplicationLoop.restore("application", null, context)).thenThrow(failure);

        ReflectionTestUtils.invokeMethod(
                coordinator,
                "executeJob",
                jobId,
                "application",
                null,
                context
        );

        assertEquals(RestoreJobStatus.FAILED, context.getStatus());
        assertEquals("boom", context.getErrorMessage());
    }

    @Test
    void alwaysRemovesJobFromRunningMap() {
        UUID jobId = UUID.randomUUID();
        RestoreJobExecutionContext context = newContext(jobId);
        addRunningJob(jobId, context);
        when(restoreReplicationLoop.restore("application", null, context))
                .thenThrow(new RuntimeException("boom"));

        ReflectionTestUtils.invokeMethod(
                coordinator,
                "executeJob",
                jobId,
                "application",
                null,
                context
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

    private void addRunningJob(UUID jobId, RestoreJobExecutionContext context) {
        runningJobs().put(jobId, context);
        jobs().put(jobId, context);
    }

    @SuppressWarnings("unchecked")
    private Map<UUID, RestoreJobExecutionContext> jobs() {
        return (Map<UUID, RestoreJobExecutionContext>) ReflectionTestUtils.getField(
                coordinator,
                "jobs"
        );
    }

    private void addJob(UUID jobId, RestoreJobExecutionContext context) {
        jobs().put(jobId, context);
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

    private RestoreJobExecutionContext newContext(UUID jobId) {
        return new RestoreJobExecutionContext(
                jobId,
                "application",
                Instant.now(),
                null
        );
    }
}
