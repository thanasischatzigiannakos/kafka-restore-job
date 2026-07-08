package com.example.kafkarestorejob.restoreengine.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface RestoreTransformer {

    String messageType();

    ProducerRecord<String, byte[]> transform(String targetTopic, ConsumerRecord<String, byte[]> sourceRecord);
}
