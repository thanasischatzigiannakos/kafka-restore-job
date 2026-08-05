package com.example.kafkarestorejob.restoreengine.kafka;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

/**
 * Forwards source payload bytes unchanged while adding restore provenance headers.
 */
@Component
public class GenericRestoreTransformer implements RestoreTransformer {

    /**
     * Creates the target record by copying the source key, payload, partition, and headers and by
     * appending restore metadata headers.
     *
     * @param restoreType the logical restore type
     * @param targetTopic the target topic
     * @param sourceRecord the source record being forwarded
     * @return the target producer record
     */
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
        targetRecord.headers().add("restore-source-topic", sourceRecord.topic().getBytes(StandardCharsets.UTF_8));
        targetRecord.headers().add(
                "restore-source-partition",
                Integer.toString(sourceRecord.partition()).getBytes(StandardCharsets.UTF_8)
        );
        targetRecord.headers().add(
                "restore-source-offset",
                Long.toString(sourceRecord.offset()).getBytes(StandardCharsets.UTF_8)
        );
        return targetRecord;
    }
}
