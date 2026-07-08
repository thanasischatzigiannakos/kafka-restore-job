package com.example.kafkarestorejob.restoreengine.verification;

import java.util.List;

public interface ChecksumExtractor {

    List<String> supportedFields();

    List<String> extractChecksums(byte[] messagePayload);
}
