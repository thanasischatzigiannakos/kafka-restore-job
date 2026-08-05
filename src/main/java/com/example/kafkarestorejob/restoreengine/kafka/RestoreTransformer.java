package com.example.kafkarestorejob.restoreengine.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Transforms one consumed source record into the producer record sent to the target topic.
 */
public interface RestoreTransformer {

    /**
     * Creates the target record for one source record.
     *
     * @param restoreType the logical restore type being executed
     * @param targetTopic the target topic receiving the restored record
     * @param sourceRecord the consumed source record
     * @return the target producer record
     */
    ProducerRecord<String, byte[]> transform(
            String restoreType,
            String targetTopic,
            ConsumerRecord<String, byte[]> sourceRecord
    );
}
