package com.example.kafkarestorejob.restoreengine.verification;

public record BinaryReference(
        String fieldPath,
        String bucketKey,
        String objectKey,
        String checksum
) {
}
