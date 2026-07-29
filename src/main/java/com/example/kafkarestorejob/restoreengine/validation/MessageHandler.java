package com.example.kafkarestorejob.restoreengine.validation;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface MessageHandler {

    String getType();

    boolean hasBinary();

    void validate(
            ConsumerRecord<String, byte[]> sourceRecord,
            RestoreRecordValidationContext context
    );
}
