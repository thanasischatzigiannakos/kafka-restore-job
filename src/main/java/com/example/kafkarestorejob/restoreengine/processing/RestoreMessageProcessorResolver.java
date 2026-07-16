package com.example.kafkarestorejob.restoreengine.processing;

import com.example.kafkarestorejob.restoreengine.inspection.RestoreMessageInspectorResolver;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreTransformerResolver;
import com.example.kafkarestorejob.restoreengine.serialization.RestoreMessageUnpackerResolver;
import org.springframework.stereotype.Component;

@Component
public class RestoreMessageProcessorResolver {

    private final RestoreMessageUnpackerResolver unpackerResolver;
    private final RestoreMessageInspectorResolver inspectorResolver;
    private final RestoreTransformerResolver transformerResolver;

    public RestoreMessageProcessorResolver(
            RestoreMessageUnpackerResolver unpackerResolver,
            RestoreMessageInspectorResolver inspectorResolver,
            RestoreTransformerResolver transformerResolver
    ) {
        this.unpackerResolver = unpackerResolver;
        this.inspectorResolver = inspectorResolver;
        this.transformerResolver = transformerResolver;
    }

    public RestoreMessageProcessor resolve(String messageType) {
        return new RestoreMessageProcessor(
                unpackerResolver.resolve(messageType),
                inspectorResolver.resolve(messageType),
                transformerResolver.resolve(messageType)
        );
    }
}
