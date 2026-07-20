package com.example.kafkarestorejob.restoreengine.verification;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.kafkarestorejob.restoreengine.config.EngineS3Properties;
import com.example.kafkarestorejob.restoreengine.s3.S3BucketResolver;
import com.example.kafkarestorejob.restoreengine.s3.S3ClientFactory;
import com.example.kafkarestorejob.restoreengine.validation.RestoreRecordValidationContext;
import com.example.kafkarestorejob.restoreengine.validation.RestoreValidationMetrics;
import java.io.ByteArrayInputStream;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

@ExtendWith(MockitoExtension.class)
class S3BinaryCompletenessValidatorTest {

    @Mock
    private S3BucketResolver bucketResolver;

    @Mock
    private S3ClientFactory clientFactory;

    @Mock
    private S3Client s3Client;

    private S3BinaryCompletenessValidator validator;
    private EngineS3Properties.BucketProperties bucketProperties;
    private RestoreRecordValidationContext context;

    @BeforeEach
    void setUp() {
        validator = new S3BinaryCompletenessValidator(
                bucketResolver,
                clientFactory,
                new RestoreValidationMetrics()
        );
        bucketProperties = new EngineS3Properties.BucketProperties();
        bucketProperties.setBucket("restore-bucket");
        context = new RestoreRecordValidationContext(
                UUID.randomUUID(),
                "application",
                "source-topic",
                1,
                99L
        );

        when(bucketResolver.resolve("default")).thenReturn(bucketProperties);
        when(clientFactory.createClient("default")).thenReturn(s3Client);
    }

    @Test
    void validatesExistingBinaryWithoutChecksumAndDoesNotStreamObject() {
        BinaryReference reference = new BinaryReference(
                "payload.file",
                "default",
                "object-key",
                Optional.of(4L),
                Optional.of("   "),
                BinaryReferencePurpose.REQUIRED_CONTENT
        );
        when(s3Client.headObject(any())).thenReturn(
                HeadObjectResponse.builder().contentLength(4L).eTag("\"unused\"").build()
        );

        validator.validate(context, "application", String.class, reference);

        verify(s3Client).headObject(any());
        verify(s3Client, never()).getObject(any());
    }

    @Test
    void throwsWhenBinaryDoesNotExist() {
        BinaryReference reference = new BinaryReference(
                "payload.file",
                "default",
                "missing-key",
                Optional.empty(),
                Optional.empty(),
                BinaryReferencePurpose.REQUIRED_CONTENT
        );
        when(s3Client.headObject(any())).thenThrow(NoSuchKeyException.builder().message("missing").build());

        assertThrows(
                BinaryNotFoundException.class,
                () -> validator.validate(context, "application", String.class, reference)
        );
    }

    @Test
    void throwsWhenSizeDoesNotMatch() {
        BinaryReference reference = new BinaryReference(
                "payload.file",
                "default",
                "object-key",
                Optional.of(10L),
                Optional.empty(),
                BinaryReferencePurpose.REQUIRED_CONTENT
        );
        when(s3Client.headObject(any())).thenReturn(
                HeadObjectResponse.builder().contentLength(4L).build()
        );

        assertThrows(
                BinarySizeMismatchException.class,
                () -> validator.validate(context, "application", String.class, reference)
        );
    }

    @Test
    void throwsWhenChecksumDoesNotMatch() {
        BinaryReference reference = new BinaryReference(
                "payload.file",
                "default",
                "object-key",
                Optional.empty(),
                Optional.of("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                BinaryReferencePurpose.REQUIRED_CONTENT
        );
        when(s3Client.headObject(any())).thenReturn(
                HeadObjectResponse.builder().contentLength(3L).eTag("\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\"").build()
        );
        when(s3Client.getObject(any())).thenReturn(responseInputStream("xyz".getBytes()));

        assertThrows(
                BinaryChecksumMismatchException.class,
                () -> validator.validate(context, "application", String.class, reference)
        );
    }

    @Test
    void throwsWhenChecksumFormatIsUnsupported() {
        BinaryReference reference = new BinaryReference(
                "payload.file",
                "default",
                "object-key",
                Optional.empty(),
                Optional.of("sha256:abc"),
                BinaryReferencePurpose.REQUIRED_CONTENT
        );
        when(s3Client.headObject(any())).thenReturn(
                HeadObjectResponse.builder().contentLength(3L).build()
        );

        assertThrows(
                UnsupportedChecksumAlgorithmException.class,
                () -> validator.validate(context, "application", String.class, reference)
        );
    }

    @Test
    void deletionReferenceSkipsS3Checks() {
        BinaryReference reference = new BinaryReference(
                "payload.deleted",
                "default",
                "deleted-key",
                Optional.empty(),
                Optional.empty(),
                BinaryReferencePurpose.DELETION_REFERENCE
        );

        validator.validate(context, "application", String.class, reference);

        verify(bucketResolver, never()).resolve("default");
        verify(clientFactory, never()).createClient("default");
    }

    private ResponseInputStream<GetObjectResponse> responseInputStream(byte[] bytes) {
        return new ResponseInputStream<>(
                GetObjectResponse.builder().contentLength((long) bytes.length).build(),
                AbortableInputStream.create(new ByteArrayInputStream(bytes))
        );
    }
}
