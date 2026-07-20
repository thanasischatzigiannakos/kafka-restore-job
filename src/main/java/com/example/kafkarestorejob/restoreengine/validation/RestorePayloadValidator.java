package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.serialization.MessageUnpackingException;
import com.example.kafkarestorejob.restoreengine.serialization.RestoreMessageUnpackerResolver;
import com.example.kafkarestorejob.restoreengine.verification.FileReferenceExtractorRegistry;
import org.springframework.stereotype.Component;

@Component
public class RestorePayloadValidator {

    private final ExpectedPayloadTypeResolver typeResolver;
    private final FileReferenceExtractorRegistry extractorRegistry;
    private final RestoreMessageUnpackerResolver unpackerResolver;
    private final RestoreValidationMetrics metrics;

    public RestorePayloadValidator(
            ExpectedPayloadTypeResolver typeResolver,
            FileReferenceExtractorRegistry extractorRegistry,
            RestoreMessageUnpackerResolver unpackerResolver,
            RestoreValidationMetrics metrics
    ) {
        this.typeResolver = typeResolver;
        this.extractorRegistry = extractorRegistry;
        this.unpackerResolver = unpackerResolver;
        this.metrics = metrics;
    }

    public void validate(
            String configuredMessageType,
            RestoreRecordValidationContext context,
            byte[] payloadBytes
    ) {
        Class<?> expectedClass = typeResolver.resolve(configuredMessageType);
        String expectedPayloadClass = expectedClass.getSimpleName();
        metrics.increment(
                "restore_payload_type_checks_total",
                context.restoreType(),
                configuredMessageType,
                expectedPayloadClass
        );

        if (!extractorRegistry.supports(expectedClass)) {
            return;
        }

        Object unpackedPayload = unpackPayload(configuredMessageType, context, payloadBytes);
        metrics.increment(
                "restore_payload_unpacks_total",
                context.restoreType(),
                configuredMessageType,
                expectedPayloadClass
        );
    }

    private Object unpackPayload(
            String configuredMessageType,
            RestoreRecordValidationContext context,
            byte[] payloadBytes
    ) {
        try {
            return unpackerResolver.unpack(configuredMessageType, payloadBytes);
        } catch (MessageUnpackingException exception) {
            throw new RestorePayloadValidationException(
                    "Failed to unpack payload for restoreType=" + context.restoreType()
                            + " sourceTopic=" + context.sourceTopic()
                            + " partition=" + context.partition()
                            + " offset=" + context.offset()
                            + " messageType=" + configuredMessageType,
                    exception
            );
        }
    }
}
