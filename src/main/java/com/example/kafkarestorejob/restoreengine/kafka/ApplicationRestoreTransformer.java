package com.example.kafkarestorejob.restoreengine.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

@Component
public class ApplicationRestoreTransformer implements RestoreTransformer {

    @Override
    public String messageType() {
        return "application";
    }

    @Override
    public ProducerRecord<String, byte[]> transform(String targetTopic, ConsumerRecord<String, byte[]> sourceRecord) {
        ProducerRecord<String, byte[]> targetRecord = new ProducerRecord<>(
                targetTopic,
                sourceRecord.partition(),
                sourceRecord.key(),
                sourceRecord.value()
        );
        sourceRecord.headers().forEach(header -> targetRecord.headers().add(header));
        targetRecord.headers().add("restore-message-type", messageType().getBytes());
        return targetRecord;
    }
}
