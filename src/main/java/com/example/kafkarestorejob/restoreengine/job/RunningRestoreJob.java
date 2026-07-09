package com.example.kafkarestorejob.restoreengine.job;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class RunningRestoreJob {

    private final AtomicBoolean cancellationRequested = new AtomicBoolean();
    private final AtomicReference<RestoreJobStatus> status = new AtomicReference<>(RestoreJobStatus.PENDING);
    private final AtomicReference<Instant> cancellationRequestedAt = new AtomicReference<>();

    public boolean isCancellationRequested() {
        return cancellationRequested.get();
    }

    public boolean requestCancellation() {
        RestoreJobStatus currentStatus = status.get();
        if (currentStatus != RestoreJobStatus.PENDING
                && currentStatus != RestoreJobStatus.RUNNING
                && currentStatus != RestoreJobStatus.CANCELLATION_REQUESTED) {
            return false;
        }
        if (cancellationRequested.compareAndSet(false, true)) {
            cancellationRequestedAt.set(Instant.now());
        }
        status.set(RestoreJobStatus.CANCELLATION_REQUESTED);
        return true;
    }

    public RestoreJobStatus getStatus() {
        return status.get();
    }

    public void setStatus(RestoreJobStatus status) {
        this.status.set(status);
    }

    public Instant getCancellationRequestedAt() {
        return cancellationRequestedAt.get();
    }
}
