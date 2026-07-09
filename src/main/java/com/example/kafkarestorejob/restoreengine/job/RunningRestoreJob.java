package com.example.kafkarestorejob.restoreengine.job;

import java.util.concurrent.atomic.AtomicBoolean;

public final class RunningRestoreJob {

    private final AtomicBoolean cancellationRequested = new AtomicBoolean();

    public boolean isCancellationRequested() {
        return cancellationRequested.get();
    }

    public void requestCancellation() {
        cancellationRequested.set(true);
    }
}
