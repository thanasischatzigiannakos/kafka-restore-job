package com.example.kafkarestorejob.restoreengine.validation;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.kafkarestorejob.restoreengine.config.EngineKafkaProperties;
import com.example.kafkarestorejob.restoreengine.serialization.MessageUnpackingException;
import com.example.kafkarestorejob.restoreengine.serialization.RestoreMessageUnpackerResolver;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RestorePayloadValidatorTest {

    @Mock
    private ExpectedPayloadTypeResolver typeResolver;

    @Mock
    private FileCapablePayloadRegistry fileCapablePayloadRegistry;

    @Mock
    private ExpectedMessageTypeCheckerRegistry checkerRegistry;

    @Mock
    private RestoreMessageUnpackerResolver unpackerResolver;

    private RestorePayloadValidator validator;
    private RestoreRecordValidationContext context;
    private EngineKafkaProperties.PipelineProperties pipeline;

    @BeforeEach
    void setUp() {
        validator = new RestorePayloadValidator(
                typeResolver,
                fileCapablePayloadRegistry,
                checkerRegistry,
                unpackerResolver
        );
        pipeline = new EngineKafkaProperties.PipelineProperties();
        pipeline.setMessageType("type-only");
        context = new RestoreRecordValidationContext(
                UUID.randomUUID(),
                "test-restore",
                "source-topic",
                0,
                42L
        );
    }

    @Test
    void typeOnlyValidationDoesNotUnpack() {
        when(typeResolver.resolve("type-only")).thenReturn(String.class);
        when(fileCapablePayloadRegistry.supports(String.class)).thenReturn(false);
        when(checkerRegistry.requireChecker("type-only")).thenReturn((validationContext, payloadBytes) -> {
        });

        validator.validate(pipeline, context, "{\"value\":1}".getBytes());

        verify(checkerRegistry).requireChecker("type-only");
        verify(unpackerResolver, never()).unpack(any(), any());
    }

    @Test
    void fileCapableValidationUnpacksOnceWithoutFurtherValidation() {
        pipeline.setMessageType("file-type");
        when(typeResolver.resolve("file-type")).thenReturn(TestPayload.class);
        when(fileCapablePayloadRegistry.supports(TestPayload.class)).thenReturn(true);
        when(unpackerResolver.unpack(eq("file-type"), any())).thenReturn(new TestPayload());

        validator.validate(pipeline, context, "{\"value\":1}".getBytes());

        verify(unpackerResolver).unpack(eq("file-type"), any());
        verify(checkerRegistry, never()).requireChecker("file-type");
    }

    @Test
    void fileCapableValidationRunsOptionalTypeCheckerAfterUnpack() {
        pipeline.setMessageType("application");
        ExpectedMessageTypeChecker checker = (validationContext, payloadBytes) -> {
        };
        when(typeResolver.resolve("application")).thenReturn(TestPayload.class);
        when(fileCapablePayloadRegistry.supports(TestPayload.class)).thenReturn(true);
        when(unpackerResolver.unpack(eq("application"), any())).thenReturn(new TestPayload());
        when(checkerRegistry.findChecker("application")).thenReturn(java.util.Optional.of(checker));

        validator.validate(pipeline, context, "{\"entityType\":\"application\"}".getBytes());

        verify(unpackerResolver).unpack(eq("application"), any());
        verify(checkerRegistry).findChecker("application");
    }

    @Test
    void unpackingFailureIsWrapped() {
        pipeline.setMessageType("file-type");
        when(typeResolver.resolve("file-type")).thenReturn(TestPayload.class);
        when(fileCapablePayloadRegistry.supports(TestPayload.class)).thenReturn(true);
        when(unpackerResolver.unpack(eq("file-type"), any()))
                .thenThrow(new MessageUnpackingException("bad payload", new RuntimeException("boom")));

        assertThrows(
                RestorePayloadValidationException.class,
                () -> validator.validate(pipeline, context, "{\"value\":1}".getBytes())
        );
    }

    private static final class TestPayload {
    }
}
