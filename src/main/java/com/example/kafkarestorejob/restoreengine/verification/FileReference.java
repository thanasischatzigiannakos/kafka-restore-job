package com.example.kafkarestorejob.restoreengine.verification;

/**
 * Normalized view of one binary reference discovered in a message payload.
 *
 * @param fieldPath the logical field path used for diagnostics
 * @param objectKey the resolved S3 object key
 * @param expectedChecksum the optional checksum expected by the payload
 */
public record FileReference(
        String fieldPath,
        String objectKey,
        String expectedChecksum
) {
}
