package com.example.kafkarestorejob.restoreengine.kafka;

/**
 * Immutable summary of one completed restore execution.
 *
 * @param restoreType the logical restore type that was executed
 * @param sourceTopic the source topic that was consumed
 * @param targetTopic the target topic that received restored records
 * @param groupId the consumer group id used for the source consumer
 * @param transactionalId the transactional id used for the target producer
 * @param messageType the logical message-handler type used during validation
 * @param transactionsCommitted the number of committed producer transactions
 * @param recordsRestored the number of source records written to the target topic
 * @param emptyPollsObserved the number of empty polls observed during execution
 */
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
