package com.example.kafkarestorejob.restoreengine.validation;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface MessageHandler {

    /**
     * Returns the logical restore type handled by this validator.
     *
     * @return the restore type key used for registry lookup
     */
    String getType();

    /**
     * Returns whether this handler type can reference external binary content.
     *
     * @return {@code true} when binary validation must run
     */
    boolean hasBinary();

    /**
     * Parses and validates one source record before it is forwarded to the target topic.
     *
     * @param sourceRecord the source record under validation
     * @param context the restore context used for diagnostics
     */
    void validate(
            ConsumerRecord<String, byte[]> sourceRecord,
            RestoreRecordValidationContext context
    );
}
