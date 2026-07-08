package com.example.kafkarestorejob.restoreengine.kafka;

public record RestoreExecutionResult(
        String restoreType,
        String sourceTopic,
        String targetTopic,
        String groupId,
        String transactionalId,
        String messageType,
        int batchesCommitted,
        long recordsRestored,
        int emptyPollsObserved
) {
}
