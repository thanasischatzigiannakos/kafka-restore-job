package com.example.kafkarestorejob.restoreengine.verification;

public record FileReference(
        String fieldPath,
        String objectKey,
        String expectedChecksum
) {
}
