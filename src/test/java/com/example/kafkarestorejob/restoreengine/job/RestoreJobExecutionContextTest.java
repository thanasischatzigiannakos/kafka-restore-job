package com.example.kafkarestorejob.restoreengine.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

class RestoreJobExecutionContextTest {

    @Test
    void marksRunningFromPending() {
        RestoreJobExecutionContext context = newContext();

        context.markRunning();

        assertEquals(RestoreJobStatus.RUNNING, context.getStatus());
    }

    @Test
    void acceptsCancellationFromPending() {
        RestoreJobExecutionContext context = newContext();

        assertTrue(context.requestCancellation());
        assertEquals(RestoreJobStatus.CANCELLATION_REQUESTED, context.getStatus());
        assertNotNull(context.getCancellationRequestedAt());
    }

    @Test
    void acceptsCancellationFromRunning() {
        RestoreJobExecutionContext context = newContext();
        context.markRunning();

        assertTrue(context.requestCancellation());
        assertEquals(RestoreJobStatus.CANCELLATION_REQUESTED, context.getStatus());
    }

    @Test
    void rejectsCancellationFromFinalizing() {
        RestoreJobExecutionContext context = runningContext();

        assertTrue(context.tryMarkFinalizing());

        assertFalse(context.requestCancellation());
        assertEquals(RestoreJobStatus.FINALIZING, context.getStatus());
    }

    @Test
    void rejectsCancellationFromCompleted() {
        RestoreJobExecutionContext context = runningContext();
        assertTrue(context.tryMarkFinalizing());
        context.markCompleted();

        assertFalse(context.requestCancellation());
        assertEquals(RestoreJobStatus.COMPLETED, context.getStatus());
    }

    @Test
    void marksFinalizingFromRunning() {
        RestoreJobExecutionContext context = runningContext();

        assertTrue(context.tryMarkFinalizing());
        assertEquals(RestoreJobStatus.FINALIZING, context.getStatus());
    }

    @Test
    void preventsFinalizingAfterCancellationRequested() {
        RestoreJobExecutionContext context = runningContext();
        context.requestCancellation();

        assertFalse(context.tryMarkFinalizing());
        assertEquals(RestoreJobStatus.CANCELLATION_REQUESTED, context.getStatus());
    }

    @Test
    void marksCompletedFromFinalizing() {
        RestoreJobExecutionContext context = runningContext();
        assertTrue(context.tryMarkFinalizing());

        context.markCompleted();

        assertEquals(RestoreJobStatus.COMPLETED, context.getStatus());
    }

    @Test
    void rejectsRunningToCompleted() {
        RestoreJobExecutionContext context = runningContext();

        assertThrows(RestoreJobStateException.class, context::markCompleted);
        assertEquals(RestoreJobStatus.RUNNING, context.getStatus());
    }

    @Test
    void concurrentCancellationAndFinalizationAllowExactlyOneTransition() throws Exception {
        RestoreJobExecutionContext context = runningContext();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            Callable<Boolean> cancellationTask = () -> {
                ready.countDown();
                start.await();
                return context.requestCancellation();
            };
            Callable<Boolean> finalizingTask = () -> {
                ready.countDown();
                start.await();
                return context.tryMarkFinalizing();
            };

            Future<Boolean> cancellationFuture = executorService.submit(cancellationTask);
            Future<Boolean> finalizingFuture = executorService.submit(finalizingTask);

            ready.await();
            start.countDown();

            boolean cancellationAccepted = cancellationFuture.get();
            boolean finalizingAccepted = finalizingFuture.get();

            assertTrue(cancellationAccepted ^ finalizingAccepted);
            if (cancellationAccepted) {
                assertEquals(RestoreJobStatus.CANCELLATION_REQUESTED, context.getStatus());
            } else {
                assertEquals(RestoreJobStatus.FINALIZING, context.getStatus());
            }
        } finally {
            executorService.shutdownNow();
        }
    }

    private RestoreJobExecutionContext runningContext() {
        RestoreJobExecutionContext context = newContext();
        context.markRunning();
        return context;
    }

    private RestoreJobExecutionContext newContext() {
        return new RestoreJobExecutionContext(
                UUID.randomUUID(),
                "application",
                Instant.now(),
                null
        );
    }
}
