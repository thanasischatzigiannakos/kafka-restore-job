package com.example.kafkarestorejob.restoreengine.validation;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.kafkarestorejob.restoreengine.serialization.MessageUnpackingException;
import com.example.kafkarestorejob.restoreengine.serialization.RestoreMessageUnpackerResolver;
import com.example.kafkarestorejob.restoreengine.verification.FileReferenceExtractorRegistry;
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
    private FileReferenceExtractorRegistry extractorRegistry;

    @Mock
    private RestoreMessageUnpackerResolver unpackerResolver;

    private RestorePayloadValidator validator;
    private RestoreRecordValidationContext context;

    @BeforeEach
    void setUp() {
        validator = new RestorePayloadValidator(
                typeResolver,
                extractorRegistry,
                unpackerResolver,
                new RestoreValidationMetrics()
        );
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
        when(extractorRegistry.supports(String.class)).thenReturn(false);

        validator.validate("type-only", context, "{\"value\":1}".getBytes());

        verify(extractorRegistry, never()).extract(any(), any());
        verify(unpackerResolver, never()).unpack(any(), any());
    }

    @Test
    void fileCapableValidationUnpacksOnceWithoutFurtherValidation() {
        when(typeResolver.resolve("file-type")).thenReturn(TestPayload.class);
        when(extractorRegistry.supports(TestPayload.class)).thenReturn(true);
        when(unpackerResolver.unpack(eq("file-type"), any())).thenReturn(new TestPayload());

        validator.validate("file-type", context, "{\"value\":1}".getBytes());

        verify(unpackerResolver).unpack(eq("file-type"), any());
        verify(extractorRegistry, never()).extract(eq(TestPayload.class), any());
    }

    @Test
    void unpackingFailureIsWrapped() {
        when(typeResolver.resolve("file-type")).thenReturn(TestPayload.class);
        when(extractorRegistry.supports(TestPayload.class)).thenReturn(true);
        when(unpackerResolver.unpack(eq("file-type"), any()))
                .thenThrow(new MessageUnpackingException("bad payload", new RuntimeException("boom")));

        assertThrows(
                RestorePayloadValidationException.class,
                () -> validator.validate("file-type", context, "{\"value\":1}".getBytes())
        );
    }

    private static final class TestPayload {
    }
}
