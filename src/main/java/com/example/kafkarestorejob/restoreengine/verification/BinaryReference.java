package com.example.kafkarestorejob.restoreengine.verification;

import java.util.Optional;

public record BinaryReference(
        String fieldPath,
        String bucketKey,
        String objectKey,
        Optional<Long> expectedSize,
        Optional<String> expectedChecksum,
        BinaryReferencePurpose purpose
) {

    public BinaryReference {
        expectedSize = expectedSize == null ? Optional.empty() : expectedSize;
        expectedChecksum = normalizeChecksum(expectedChecksum);
        purpose = purpose == null ? BinaryReferencePurpose.REQUIRED_CONTENT : purpose;
    }

    private static Optional<String> normalizeChecksum(Optional<String> checksum) {
        if (checksum == null || checksum.isEmpty()) {
            return Optional.empty();
        }
        String normalized = checksum.orElseThrow().trim().toLowerCase();
        if (normalized.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(normalized);
    }
}
