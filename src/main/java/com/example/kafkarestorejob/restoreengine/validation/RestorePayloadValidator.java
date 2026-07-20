package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.serialization.MessageUnpackingException;
import com.example.kafkarestorejob.restoreengine.serialization.RestoreMessageUnpackerResolver;
import org.springframework.stereotype.Component;

@Component
public class RestorePayloadValidator {

    private final ExpectedPayloadTypeResolver typeResolver;
    private final FileCapablePayloadRegistry fileCapablePayloadRegistry;
    private final ExpectedMessageTypeCheckerRegistry checkerRegistry;
    private final RestoreMessageUnpackerResolver unpackerResolver;

    public RestorePayloadValidator(
            ExpectedPayloadTypeResolver typeResolver,
            FileCapablePayloadRegistry fileCapablePayloadRegistry,
            ExpectedMessageTypeCheckerRegistry checkerRegistry,
            RestoreMessageUnpackerResolver unpackerResolver
    ) {
        this.typeResolver = typeResolver;
        this.fileCapablePayloadRegistry = fileCapablePayloadRegistry;
        this.checkerRegistry = checkerRegistry;
        this.unpackerResolver = unpackerResolver;
    }

    public void validate(
            String configuredMessageType,
            RestoreRecordValidationContext context,
            byte[] payloadBytes
    ) {
        Class<?> expectedClass = typeResolver.resolve(configuredMessageType);
        if (!fileCapablePayloadRegistry.supports(expectedClass)) {
            checkerRegistry.requireChecker(configuredMessageType).validate(context, payloadBytes);
            return;
        }

        unpackPayload(configuredMessageType, context, payloadBytes);
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
