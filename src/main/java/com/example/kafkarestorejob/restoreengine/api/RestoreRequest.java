package com.example.kafkarestorejob.restoreengine.api;

import jakarta.validation.constraints.NotBlank;
import java.time.Instant;

/**
 * API request used to start a restore job.
 */
public class RestoreRequest {

    @NotBlank
    private String restoreType;

    private Instant restoreFromTimestamp;

    /**
     * Returns the logical restore type to execute.
     *
     * @return the restore type key
     */
    public String getRestoreType() {
        return restoreType;
    }

    /**
     * Sets the logical restore type to execute.
     *
     * @param restoreType the restore type key
     */
    public void setRestoreType(String restoreType) {
        this.restoreType = restoreType;
    }

    /**
     * Returns the optional timestamp used to position the source consumer.
     *
     * @return the restore-from timestamp, or {@code null} when timestamp-based seeking is not
     *         requested
     */
    public Instant getRestoreFromTimestamp() {
        return restoreFromTimestamp;
    }

    /**
     * Sets the optional timestamp used to position the source consumer.
     *
     * @param restoreFromTimestamp the restore-from timestamp
     */
    public void setRestoreFromTimestamp(Instant restoreFromTimestamp) {
        this.restoreFromTimestamp = restoreFromTimestamp;
    }
}
