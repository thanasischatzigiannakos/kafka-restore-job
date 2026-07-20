package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.serialization.MessageUnpackingException;
import com.example.kafkarestorejob.restoreengine.serialization.RestoreMessageUnpackerResolver;
import com.example.kafkarestorejob.restoreengine.verification.BinaryCompletenessValidator;
import com.example.kafkarestorejob.restoreengine.verification.BinaryReference;
import com.example.kafkarestorejob.restoreengine.verification.FileReferenceExtractorRegistry;
import java.util.Collection;
import org.springframework.stereotype.Component;

@Component
public class RestorePayloadValidator {

    private final ExpectedPayloadTypeResolver typeResolver;
    private final FileReferenceExtractorRegistry extractorRegistry;
    private final RestoreMessageUnpackerResolver unpackerResolver;
    private final BinaryCompletenessValidator binaryCompletenessValidator;
    private final RestoreValidationMetrics metrics;

    public RestorePayloadValidator(
            ExpectedPayloadTypeResolver typeResolver,
            FileReferenceExtractorRegistry extractorRegistry,
            RestoreMessageUnpackerResolver unpackerResolver,
            BinaryCompletenessValidator binaryCompletenessValidator,
            RestoreValidationMetrics metrics
    ) {
        this.typeResolver = typeResolver;
        this.extractorRegistry = extractorRegistry;
        this.unpackerResolver = unpackerResolver;
        this.binaryCompletenessValidator = binaryCompletenessValidator;
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

        Collection<BinaryReference> references = extractorRegistry.extract(expectedClass, unpackedPayload);
        metrics.increment(
                "restore_file_references_total",
                context.restoreType(),
                configuredMessageType,
                expectedPayloadClass,
                references.size()
        );
        for (BinaryReference reference : references) {
            binaryCompletenessValidator.validate(context, configuredMessageType, expectedClass, reference);
        }
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
