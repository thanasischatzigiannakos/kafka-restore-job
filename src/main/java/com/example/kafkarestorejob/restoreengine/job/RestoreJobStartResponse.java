package com.example.kafkarestorejob.restoreengine.job;

import java.util.UUID;

/**
 * API response returned when a restore job has been accepted for execution.
 *
 * @param id the restore job identifier
 */
public record RestoreJobStartResponse(UUID id) {
}
