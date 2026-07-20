package com.example.kafkarestorejob.restoreengine.validation;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.kafkarestorejob.restoreengine.serialization.MessageUnpackingException;
import com.example.kafkarestorejob.restoreengine.serialization.RestoreMessageUnpackerResolver;
import com.example.kafkarestorejob.restoreengine.verification.BinaryCompletenessValidator;
import com.example.kafkarestorejob.restoreengine.verification.BinaryReference;
import com.example.kafkarestorejob.restoreengine.verification.BinaryReferencePurpose;
import com.example.kafkarestorejob.restoreengine.verification.FileReferenceExtractorRegistry;
import java.util.List;
import java.util.Optional;
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

    @Mock
    private BinaryCompletenessValidator binaryCompletenessValidator;

    private RestorePayloadValidator validator;
    private RestoreRecordValidationContext context;

    @BeforeEach
    void setUp() {
        validator = new RestorePayloadValidator(
                typeResolver,
                extractorRegistry,
                unpackerResolver,
                binaryCompletenessValidator,
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
        verify(binaryCompletenessValidator, never()).validate(any(), any(), any(), any());
    }

    @Test
    void fileCapableValidationUnpacksOnceAndValidatesAllReferences() {
        BinaryReference first = new BinaryReference(
                "payload.files[0]",
                "default",
                "first-key",
                Optional.empty(),
                Optional.of("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                BinaryReferencePurpose.REQUIRED_CONTENT
        );
        BinaryReference second = new BinaryReference(
                "payload.files[1]",
                "default",
                "second-key",
                Optional.empty(),
                Optional.empty(),
                BinaryReferencePurpose.REQUIRED_CONTENT
        );

        when(typeResolver.resolve("file-type")).thenReturn(TestPayload.class);
        when(extractorRegistry.supports(TestPayload.class)).thenReturn(true);
        when(unpackerResolver.unpack(eq("file-type"), any())).thenReturn(new TestPayload());
        when(extractorRegistry.extract(eq(TestPayload.class), any())).thenReturn(List.of(first, second));

        validator.validate("file-type", context, "{\"value\":1}".getBytes());

        verify(unpackerResolver).unpack(eq("file-type"), any());
        verify(extractorRegistry).extract(eq(TestPayload.class), any());
        verify(binaryCompletenessValidator).validate(context, "file-type", TestPayload.class, first);
        verify(binaryCompletenessValidator).validate(context, "file-type", TestPayload.class, second);
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
