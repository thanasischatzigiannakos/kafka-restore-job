package com.example.kafkarestorejob.restoreengine.validation;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.kafkarestorejob.restoreengine.verification.FileReference;
import com.example.kafkarestorejob.restoreengine.verification.S3FileExistenceVerifier;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RestorePayloadValidatorTest {

    @Mock
    private RestorePayloadHandlerRegistry handlerRegistry;

    @Mock
    private S3FileExistenceVerifier s3FileExistenceVerifier;

    @Mock
    private RestorePayloadHandler handler;

    private RestorePayloadValidator validator;
    private RestoreRecordValidationContext context;

    @BeforeEach
    void setUp() {
        validator = new RestorePayloadValidator(
                handlerRegistry,
                s3FileExistenceVerifier
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
    void delegatesValidationToHandlerForRestoreType() {
        when(handlerRegistry.requireHandler("test-restore")).thenReturn(handler);
        when(handler.validateAndExtractFileReferences(any(), any())).thenReturn(List.of());

        validator.validate(context, "{\"value\":1}".getBytes());

        verify(handlerRegistry).requireHandler("test-restore");
        verify(handler).validateAndExtractFileReferences(any(), any());
    }

    @Test
    void validatesS3ExistenceForEachExtractedReference() {
        FileReference first = new FileReference("payload.files[0]", "first-key");
        FileReference second = new FileReference("payload.files[1]", "second-key");
        when(handlerRegistry.requireHandler("test-restore")).thenReturn(handler);
        when(handler.validateAndExtractFileReferences(any(), any())).thenReturn(List.of(first, second));

        validator.validate(context, "{\"value\":1}".getBytes());

        verify(s3FileExistenceVerifier).verifyExists(context, "test-restore", first);
        verify(s3FileExistenceVerifier).verifyExists(context, "test-restore", second);
    }

    @Test
    void propagatesHandlerValidationFailures() {
        when(handlerRegistry.requireHandler("test-restore")).thenReturn(handler);
        when(handler.validateAndExtractFileReferences(any(), any()))
                .thenThrow(new RestorePayloadValidationException("bad payload"));

        assertThrows(
                RestorePayloadValidationException.class,
                () -> validator.validate(context, "{\"value\":1}".getBytes())
        );
    }
}
