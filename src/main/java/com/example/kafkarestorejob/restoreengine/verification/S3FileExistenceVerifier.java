package com.example.kafkarestorejob.restoreengine.verification;

import com.example.kafkarestorejob.restoreengine.config.EngineS3Properties;
import com.example.kafkarestorejob.restoreengine.s3.S3ClientFactory;
import com.example.kafkarestorejob.restoreengine.validation.RestorePayloadValidationException;
import com.example.kafkarestorejob.restoreengine.validation.RestoreRecordValidationContext;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Validates that binary references point to existing S3 objects and, when provided, that the
 * object checksum matches the expected payload checksum.
 */
@Component
public class S3FileExistenceVerifier {

    private static final Logger log = LoggerFactory.getLogger(S3FileExistenceVerifier.class);
    private static final HexFormat HEX_FORMAT = HexFormat.of();
    private static final int BUFFER_SIZE = 8192;

    private final String bucket;
    private final S3Client client;

    /**
     * Creates the verifier and its underlying S3 client.
     *
     * @param properties the S3 settings containing the bucket name
     * @param clientFactory the client factory used to create the S3 client
     */
    public S3FileExistenceVerifier(
            EngineS3Properties properties,
            S3ClientFactory clientFactory
    ) {
        this.bucket = properties.getBucket();
        this.client = clientFactory.createClient();
    }

    /**
     * Verifies object existence and optionally validates the expected checksum.
     *
     * @param context the restore context used for diagnostics
     * @param messageType the logical message type under validation
     * @param reference the referenced S3 object
     */
    public void verifyExists(
            RestoreRecordValidationContext context,
            String messageType,
            FileReference reference
    ) {
        HeadObjectResponse metadata = headObject(context, messageType, reference);
        Optional<byte[]> expectedChecksum = normalizeExpectedChecksum(reference.expectedChecksum());
        if (expectedChecksum.isEmpty()) {
            log.debug(
                    "Validated binary existence without checksum restoreType={} messageType={} fieldPath={} objectKey={}",
                    context.restoreType(),
                    messageType,
                    reference.fieldPath(),
                    reference.objectKey()
            );
            return;
        }

        byte[] actualChecksum = actualChecksum(reference.objectKey(), metadata, context, messageType, reference);
        if (!MessageDigest.isEqual(expectedChecksum.get(), actualChecksum)) {
            throw new RestorePayloadValidationException(
                    "Binary checksum mismatch for restoreType=" + context.restoreType()
                            + " sourceTopic=" + context.sourceTopic()
                            + " partition=" + context.partition()
                            + " offset=" + context.offset()
                            + " messageType=" + messageType
                            + " fieldPath=" + reference.fieldPath()
                            + " objectKey=" + reference.objectKey()
            );
        }
    }

    /**
     * Loads metadata for the referenced object using {@code headObject}.
     *
     * @param context the restore context used for diagnostics
     * @param messageType the logical message type under validation
     * @param reference the referenced S3 object
     * @return the object metadata
     */
    private HeadObjectResponse headObject(
            RestoreRecordValidationContext context,
            String messageType,
            FileReference reference
    ) {
        try {
            return client.headObject(HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(reference.objectKey())
                    .build());
        } catch (NoSuchKeyException exception) {
            throw missingBinary(context, messageType, reference, exception);
        } catch (S3Exception exception) {
            if (exception.statusCode() == 404) {
                throw missingBinary(context, messageType, reference, exception);
            }
            throw new RestorePayloadValidationException(
                    "Unable to validate binary object for restoreType=" + context.restoreType()
                            + " sourceTopic=" + context.sourceTopic()
                            + " partition=" + context.partition()
                            + " offset=" + context.offset()
                            + " messageType=" + messageType
                            + " fieldPath=" + reference.fieldPath()
                            + " objectKey=" + reference.objectKey(),
                    exception
            );
        }
    }

    /**
     * Creates the validation exception used when the referenced binary is missing.
     *
     * @param context the restore context used for diagnostics
     * @param messageType the logical message type under validation
     * @param reference the referenced S3 object
     * @param exception the originating S3 exception
     * @return the wrapped validation exception
     */
    private RestorePayloadValidationException missingBinary(
            RestoreRecordValidationContext context,
            String messageType,
            FileReference reference,
            Exception exception
    ) {
        return new RestorePayloadValidationException(
                "Missing file for restoreType=" + context.restoreType()
                        + " sourceTopic=" + context.sourceTopic()
                        + " partition=" + context.partition()
                        + " offset=" + context.offset()
                        + " messageType=" + messageType
                        + " fieldPath=" + reference.fieldPath()
                        + " objectKey=" + reference.objectKey(),
                exception
        );
    }

    /**
     * Resolves the object's actual SHA-256 either from metadata or by streaming the object body.
     *
     * @param objectKey the S3 object key
     * @param metadata the metadata loaded via {@code headObject}
     * @param context the restore context used for diagnostics
     * @param messageType the logical message type under validation
     * @param reference the referenced S3 object
     * @return the actual checksum bytes
     */
    private byte[] actualChecksum(
            String objectKey,
            HeadObjectResponse metadata,
            RestoreRecordValidationContext context,
            String messageType,
            FileReference reference
    ) {
        String checksumSha256 = metadata.checksumSHA256();
        if (checksumSha256 != null && !checksumSha256.isBlank()) {
            return Base64.getDecoder().decode(checksumSha256.trim());
        }
        return streamSha256(objectKey, context, messageType, reference);
    }

    /**
     * Streams the object body and calculates its SHA-256 checksum without loading the entire body
     * into memory.
     *
     * @param objectKey the S3 object key
     * @param context the restore context used for diagnostics
     * @param messageType the logical message type under validation
     * @param reference the referenced S3 object
     * @return the calculated checksum bytes
     */
    private byte[] streamSha256(
            String objectKey,
            RestoreRecordValidationContext context,
            String messageType,
            FileReference reference
    ) {
        MessageDigest digest = sha256Digest();
        try (ResponseInputStream<GetObjectResponse> input = client.getObject(
                GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(objectKey)
                        .build())) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = input.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesRead);
            }
            return digest.digest();
        } catch (NoSuchKeyException exception) {
            throw missingBinary(context, messageType, reference, exception);
        } catch (S3Exception exception) {
            if (exception.statusCode() == 404) {
                throw missingBinary(context, messageType, reference, exception);
            }
            throw new RestorePayloadValidationException(
                    "Unable to stream binary object for checksum validation restoreType="
                            + context.restoreType()
                            + " sourceTopic=" + context.sourceTopic()
                            + " partition=" + context.partition()
                            + " offset=" + context.offset()
                            + " messageType=" + messageType
                            + " fieldPath=" + reference.fieldPath()
                            + " objectKey=" + reference.objectKey(),
                    exception
            );
        } catch (IOException exception) {
            throw new RestorePayloadValidationException(
                    "Unable to read binary object for checksum validation restoreType="
                            + context.restoreType()
                            + " sourceTopic=" + context.sourceTopic()
                            + " partition=" + context.partition()
                            + " offset=" + context.offset()
                            + " messageType=" + messageType
                            + " fieldPath=" + reference.fieldPath()
                            + " objectKey=" + reference.objectKey(),
                    exception
            );
        }
    }

    /**
     * Normalizes the expected checksum text from the payload into raw SHA-256 bytes.
     *
     * @param checksum the payload checksum text
     * @return the normalized checksum bytes, or empty when the payload omitted a checksum
     */
    private Optional<byte[]> normalizeExpectedChecksum(String checksum) {
        if (checksum == null) {
            return Optional.empty();
        }

        String normalized = checksum.trim();
        if (normalized.isEmpty()) {
            return Optional.empty();
        }

        String withoutPrefix = stripSha256Prefix(stripQuotes(normalized));
        if (isHex(withoutPrefix)) {
            return Optional.of(HEX_FORMAT.parseHex(withoutPrefix.toLowerCase(Locale.ROOT)));
        }

        try {
            return Optional.of(Base64.getDecoder().decode(withoutPrefix));
        } catch (IllegalArgumentException exception) {
            throw new RestorePayloadValidationException(
                    "Unsupported checksum format. Expected SHA-256 in hex or Base64."
            );
        }
    }

    /**
     * Removes one pair of wrapping quotes from a checksum value.
     *
     * @param value the raw checksum value
     * @return the unquoted checksum
     */
    private String stripQuotes(String value) {
        if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1).trim();
        }
        return value;
    }

    /**
     * Removes supported SHA-256 textual prefixes from a checksum value.
     *
     * @param value the raw checksum value
     * @return the checksum without an algorithm prefix
     */
    private String stripSha256Prefix(String value) {
        String lower = value.toLowerCase(Locale.ROOT);
        if (lower.startsWith("sha-256:")) {
            return value.substring("sha-256:".length()).trim();
        }
        if (lower.startsWith("sha256:")) {
            return value.substring("sha256:".length()).trim();
        }
        return value;
    }

    /**
     * Returns whether the supplied string is valid even-length hexadecimal text.
     *
     * @param value the candidate checksum text
     * @return {@code true} when the text is valid hexadecimal
     */
    private boolean isHex(String value) {
        if ((value.length() & 1) != 0 || value.isEmpty()) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            boolean hexDigit = (character >= '0' && character <= '9')
                    || (character >= 'a' && character <= 'f')
                    || (character >= 'A' && character <= 'F');
            if (!hexDigit) {
                return false;
            }
        }
        return true;
    }

    /**
     * Creates a SHA-256 message digest for checksum calculation.
     *
     * @return the digest instance
     */
    private MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new RestorePayloadValidationException("SHA-256 is not available", exception);
        }
    }
}
