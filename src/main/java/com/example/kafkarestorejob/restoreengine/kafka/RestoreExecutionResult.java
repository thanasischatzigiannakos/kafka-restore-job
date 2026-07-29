package com.example.kafkarestorejob.restoreengine.kafka;

public record RestoreExecutionResult(
        String restoreType,
        String sourceTopic,
        String targetTopic,
        String groupId,
        String transactionalId,
        String messageType,
        int transactionsCommitted,
        long recordsRestored,
        int emptyPollsObserved
) {
}
