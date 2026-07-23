package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.config.EngineKafkaProperties;
import com.example.kafkarestorejob.restoreengine.serialization.MessageUnpackingException;
import com.example.kafkarestorejob.restoreengine.serialization.RestoreMessageUnpackerResolver;
import com.example.kafkarestorejob.restoreengine.verification.FileReference;
import com.example.kafkarestorejob.restoreengine.verification.FileReferenceExtractorRegistry;
import com.example.kafkarestorejob.restoreengine.verification.S3FileExistenceVerifier;
import java.util.Collection;
import org.springframework.stereotype.Component;

@Component
public class RestorePayloadValidator {

    private final ExpectedPayloadTypeResolver typeResolver;
    private final FileCapablePayloadRegistry fileCapablePayloadRegistry;
    private final ExpectedMessageTypeCheckerRegistry checkerRegistry;
    private final RestoreMessageUnpackerResolver unpackerResolver;
    private final FileReferenceExtractorRegistry fileReferenceExtractorRegistry;
    private final S3FileExistenceVerifier s3FileExistenceVerifier;

    public RestorePayloadValidator(
            ExpectedPayloadTypeResolver typeResolver,
            FileCapablePayloadRegistry fileCapablePayloadRegistry,
            ExpectedMessageTypeCheckerRegistry checkerRegistry,
            RestoreMessageUnpackerResolver unpackerResolver,
            FileReferenceExtractorRegistry fileReferenceExtractorRegistry,
            S3FileExistenceVerifier s3FileExistenceVerifier
    ) {
        this.typeResolver = typeResolver;
        this.fileCapablePayloadRegistry = fileCapablePayloadRegistry;
        this.checkerRegistry = checkerRegistry;
        this.unpackerResolver = unpackerResolver;
        this.fileReferenceExtractorRegistry = fileReferenceExtractorRegistry;
        this.s3FileExistenceVerifier = s3FileExistenceVerifier;
    }

    public void validate(
            EngineKafkaProperties.PipelineProperties pipeline,
            RestoreRecordValidationContext context,
            byte[] payloadBytes
    ) {
        String configuredMessageType = pipeline.getType();
        Class<?> expectedClass = typeResolver.resolve(configuredMessageType);
        if (!fileCapablePayloadRegistry.supports(expectedClass)) {
            checkerRegistry.requireChecker(configuredMessageType).validate(context, payloadBytes);
            return;
        }

        Object unpackedPayload = unpackPayload(configuredMessageType, context, payloadBytes);
        checkerRegistry.findChecker(configuredMessageType)
                .ifPresent(checker -> checker.validate(context, payloadBytes));
        Collection<FileReference> references =
                fileReferenceExtractorRegistry.extract(expectedClass, unpackedPayload);
        for (FileReference reference : references) {
            s3FileExistenceVerifier.verifyExists(context, configuredMessageType, reference);
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
