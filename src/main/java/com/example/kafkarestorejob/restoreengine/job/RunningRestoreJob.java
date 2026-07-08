package com.example.kafkarestorejob.restoreengine.job;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class RunningRestoreJob {

    private final AtomicBoolean cancellationRequested = new AtomicBoolean(false);
    private volatile Future<?> future;

    public boolean isCancellationRequested() {
        return cancellationRequested.get();
    }

    public void requestCancellation() {
        cancellationRequested.set(true);
    }

    public Future<?> getFuture() {
        return future;
    }

    public void setFuture(Future<?> future) {
        this.future = future;
    }
}
