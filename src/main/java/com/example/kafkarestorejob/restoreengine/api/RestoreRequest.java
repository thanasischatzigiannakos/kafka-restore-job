package com.example.kafkarestorejob.restoreengine.api;

import jakarta.validation.constraints.NotBlank;
import java.time.Instant;

public class RestoreRequest {

    @NotBlank
    private String restoreType;

    private Instant restoreFromTimestamp;

    public String getRestoreType() {
        return restoreType;
    }

    public void setRestoreType(String restoreType) {
        this.restoreType = restoreType;
    }

    public Instant getRestoreFromTimestamp() {
        return restoreFromTimestamp;
    }

    public void setRestoreFromTimestamp(Instant restoreFromTimestamp) {
        this.restoreFromTimestamp = restoreFromTimestamp;
    }
}
