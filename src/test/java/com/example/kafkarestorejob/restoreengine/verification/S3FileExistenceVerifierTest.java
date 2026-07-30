package com.example.kafkarestorejob.restoreengine.verification;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.kafkarestorejob.restoreengine.config.EngineS3Properties;
import com.example.kafkarestorejob.restoreengine.s3.S3ClientFactory;
import com.example.kafkarestorejob.restoreengine.validation.RestorePayloadValidationException;
import com.example.kafkarestorejob.restoreengine.validation.RestoreRecordValidationContext;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

class S3FileExistenceVerifierTest {

    private static final RestoreRecordValidationContext CONTEXT =
            new RestoreRecordValidationContext(
                    UUID.randomUUID(),
                    "application",
                    "source-topic",
                    0,
                    42L
            );

    private S3Client client;
    private S3FileExistenceVerifier verifier;

    @BeforeEach
    void setUp() {
        client = mock(S3Client.class);
        EngineS3Properties properties = new EngineS3Properties();
        properties.setBucket("restore-bucket");
        S3ClientFactory clientFactory = mock(S3ClientFactory.class);
        when(clientFactory.createClient()).thenReturn(client);
        verifier = new S3FileExistenceVerifier(properties, clientFactory);
    }

    @Test
    void validatesExistenceOnlyWhenChecksumMissing() {
        when(client.headObject(org.mockito.ArgumentMatchers.any()))
                .thenReturn(HeadObjectResponse.builder().build());

        assertDoesNotThrow(() -> verifier.verifyExists(
                CONTEXT,
                "application",
                new FileReference("field.path", "object-key", "   ")
        ));

        verify(client, never()).getObject(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void validatesChecksumFromS3MetadataWhenAvailable() throws Exception {
        byte[] digest = sha256("payload");
        when(client.headObject(org.mockito.ArgumentMatchers.any()))
                .thenReturn(HeadObjectResponse.builder()
                        .checksumSHA256(Base64.getEncoder().encodeToString(digest))
                        .build());

        assertDoesNotThrow(() -> verifier.verifyExists(
                CONTEXT,
                "application",
                new FileReference("field.path", "object-key", Base64.getEncoder().encodeToString(digest))
        ));

        verify(client, never()).getObject(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void streamsObjectWhenChecksumMetadataIsUnavailable() throws Exception {
        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        when(client.headObject(org.mockito.ArgumentMatchers.any()))
                .thenReturn(HeadObjectResponse.builder().build());
        when(client.getObject(org.mockito.ArgumentMatchers.any()))
                .thenReturn(new ResponseInputStream<>(
                        GetObjectResponse.builder().build(),
                        AbortableInputStream.create(new ByteArrayInputStream(payload))
                ));

        assertDoesNotThrow(() -> verifier.verifyExists(
                CONTEXT,
                "application",
                new FileReference("field.path", "object-key", Base64.getEncoder().encodeToString(sha256(payload)))
        ));

        verify(client).getObject(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void throwsWhenChecksumDoesNotMatch() {
        when(client.headObject(org.mockito.ArgumentMatchers.any()))
                .thenReturn(HeadObjectResponse.builder()
                        .checksumSHA256(Base64.getEncoder().encodeToString(new byte[]{1, 2, 3}))
                        .build());

        assertThrows(RestorePayloadValidationException.class, () -> verifier.verifyExists(
                CONTEXT,
                "application",
                new FileReference("field.path", "object-key", Base64.getEncoder().encodeToString(new byte[]{9, 9, 9}))
        ));
    }

    @Test
    void throwsMissingFileFor404() {
        when(client.headObject(org.mockito.ArgumentMatchers.any()))
                .thenThrow(NoSuchKeyException.builder().message("missing").build());

        assertThrows(RestorePayloadValidationException.class, () -> verifier.verifyExists(
                CONTEXT,
                "application",
                new FileReference("field.path", "object-key", null)
        ));
    }

    @Test
    void throwsGenericValidationFailureForNon404S3Error() {
        when(client.headObject(org.mockito.ArgumentMatchers.any()))
                .thenThrow(S3Exception.builder().statusCode(500).message("boom").build());

        assertThrows(RestorePayloadValidationException.class, () -> verifier.verifyExists(
                CONTEXT,
                "application",
                new FileReference("field.path", "object-key", null)
        ));
    }

    private byte[] sha256(String value) throws Exception {
        return sha256(value.getBytes(StandardCharsets.UTF_8));
    }

    private byte[] sha256(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }
}
