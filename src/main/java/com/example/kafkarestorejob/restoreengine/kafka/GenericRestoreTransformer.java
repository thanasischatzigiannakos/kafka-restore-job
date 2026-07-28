package com.example.kafkarestorejob.restoreengine.kafka;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

@Component
public class GenericRestoreTransformer implements RestoreTransformer {

    @Override
    public ProducerRecord<String, byte[]> transform(
            String restoreType,
            String targetTopic,
            ConsumerRecord<String, byte[]> sourceRecord
    ) {
        ProducerRecord<String, byte[]> targetRecord = new ProducerRecord<>(
                targetTopic,
                sourceRecord.partition(),
                sourceRecord.key(),
                sourceRecord.value()
        );
        sourceRecord.headers().forEach(header -> targetRecord.headers().add(header));
        targetRecord.headers().add("restore-message-type", restoreType.getBytes(StandardCharsets.UTF_8));
        return targetRecord;
    }
}
