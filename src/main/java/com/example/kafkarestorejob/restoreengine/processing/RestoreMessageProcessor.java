package com.example.kafkarestorejob.restoreengine.processing;

import com.example.kafkarestorejob.restoreengine.inspection.RestoreMessageInspector;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreTransformer;
import com.example.kafkarestorejob.restoreengine.serialization.RestoreMessageUnpacker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class RestoreMessageProcessor {

    private final RestoreMessageUnpacker<?> unpacker;
    private final RestoreMessageInspector<?> inspector;
    private final RestoreTransformer transformer;

    public RestoreMessageProcessor(
            RestoreMessageUnpacker<?> unpacker,
            RestoreMessageInspector<?> inspector,
            RestoreTransformer transformer
    ) {
        this.unpacker = unpacker;
        this.inspector = inspector;
        this.transformer = transformer;
    }

    public void inspect(String restoreType, byte[] payload) {
        Object unpackedPayload = unpacker.unpack(payload);
        inspector.inspectUntyped(restoreType, unpackedPayload);
    }

    public ProducerRecord<String, byte[]> transform(
            String targetTopic,
            ConsumerRecord<String, byte[]> sourceRecord
    ) {
        return transformer.transform(targetTopic, sourceRecord);
    }
}
