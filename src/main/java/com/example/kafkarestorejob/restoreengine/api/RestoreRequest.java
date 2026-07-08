package com.example.kafkarestorejob.restoreengine.api;

import jakarta.validation.constraints.NotBlank;

public class RestoreRequest {

    @NotBlank
    private String restoreType;

    public String getRestoreType() {
        return restoreType;
    }

    public void setRestoreType(String restoreType) {
        this.restoreType = restoreType;
    }
}
