package com.example.kafkarestorejob.restoreengine.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface RestoreTransformer {

    ProducerRecord<String, byte[]> transform(
            String restoreType,
            String targetTopic,
            ConsumerRecord<String, byte[]> sourceRecord
    );
}
