package com.example.kafkarestorejob.restoreengine.verification;

import com.example.kafkarestorejob.restoreengine.config.EngineS3Properties;
import com.example.kafkarestorejob.restoreengine.s3.S3BucketResolver;
import com.example.kafkarestorejob.restoreengine.s3.S3ClientFactory;
import com.example.kafkarestorejob.restoreengine.validation.RestoreRecordValidationContext;
import com.example.kafkarestorejob.restoreengine.validation.RestoreValidationMetrics;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

@Service
public class S3BinaryCompletenessValidator implements BinaryCompletenessValidator {

    private static final Logger log = LoggerFactory.getLogger(S3BinaryCompletenessValidator.class);

    private final S3BucketResolver bucketResolver;
    private final S3ClientFactory clientFactory;
    private final RestoreValidationMetrics metrics;

    public S3BinaryCompletenessValidator(
            S3BucketResolver bucketResolver,
            S3ClientFactory clientFactory,
            RestoreValidationMetrics metrics
    ) {
        this.bucketResolver = bucketResolver;
        this.clientFactory = clientFactory;
        this.metrics = metrics;
    }

    @Override
    public void validate(
            RestoreRecordValidationContext context,
            String configuredMessageType,
            Class<?> payloadClass,
            BinaryReference reference
    ) {
        if (reference.purpose() == BinaryReferencePurpose.DELETION_REFERENCE) {
            return;
        }

        String expectedPayloadClass = payloadClass.getSimpleName();
        EngineS3Properties.BucketProperties bucketProperties = bucketResolver.resolve(reference.bucketKey());
        try (S3Client client = clientFactory.createClient(reference.bucketKey())) {
            HeadObjectResponse head = headObject(context, payloadClass, reference, bucketProperties, client);
            metrics.increment(
                    "restore_binary_exists_checks_total",
                    context.restoreType(),
                    configuredMessageType,
                    expectedPayloadClass
            );
            validateSize(context, configuredMessageType, payloadClass, reference, head, expectedPayloadClass);
            validateChecksum(
                    context,
                    configuredMessageType,
                    payloadClass,
                    reference,
                    bucketProperties,
                    client,
                    head,
                    expectedPayloadClass
            );
        }
    }

    private HeadObjectResponse headObject(
            RestoreRecordValidationContext context,
            Class<?> payloadClass,
            BinaryReference reference,
            EngineS3Properties.BucketProperties bucketProperties,
            S3Client client
    ) {
        try {
            return client.headObject(HeadObjectRequest.builder()
                    .bucket(bucketProperties.getBucket())
                    .key(reference.objectKey())
                    .build());
        } catch (NoSuchKeyException | S3Exception exception) {
            throw new BinaryNotFoundException(buildReferenceContext(
                    context,
                    payloadClass,
                    reference,
                    "Missing binary bucket=" + bucketProperties.getBucket()
            ), exception);
        }
    }

    private void validateSize(
            RestoreRecordValidationContext context,
            String configuredMessageType,
            Class<?> payloadClass,
            BinaryReference reference,
            HeadObjectResponse head,
            String expectedPayloadClass
    ) {
        if (reference.expectedSize().isEmpty()) {
            return;
        }

        metrics.increment(
                "restore_binary_size_checks_total",
                context.restoreType(),
                configuredMessageType,
                expectedPayloadClass
        );
        long actualSize = head.contentLength();
        long expectedSize = reference.expectedSize().orElseThrow();
        if (actualSize != expectedSize) {
            metrics.increment(
                    "restore_binary_validation_failures_total",
                    context.restoreType(),
                    configuredMessageType,
                    expectedPayloadClass
            );
            throw new BinarySizeMismatchException(
                    buildReferenceContext(
                            context,
                            payloadClass,
                            reference,
                            "Binary size mismatch expectedSize=" + expectedSize + " actualSize=" + actualSize
                    )
            );
        }
    }

    private void validateChecksum(
            RestoreRecordValidationContext context,
            String configuredMessageType,
            Class<?> payloadClass,
            BinaryReference reference,
            EngineS3Properties.BucketProperties bucketProperties,
            S3Client client,
            HeadObjectResponse head,
            String expectedPayloadClass
    ) {
        Optional<String> expectedChecksum = reference.expectedChecksum();
        if (expectedChecksum.isEmpty()) {
            metrics.increment(
                    "restore_binary_checksum_missing_total",
                    context.restoreType(),
                    configuredMessageType,
                    expectedPayloadClass
            );
            log.debug(
                    "Checksum unavailable for restoreType={} sourceTopic={} partition={} offset={} fieldPath={} objectKey={}",
                    context.restoreType(),
                    context.sourceTopic(),
                    context.partition(),
                    context.offset(),
                    reference.fieldPath(),
                    reference.objectKey()
            );
            return;
        }

        if (!expectedChecksum.orElseThrow().matches("[0-9a-f]{32}")) {
            metrics.increment(
                    "restore_binary_validation_failures_total",
                    context.restoreType(),
                    configuredMessageType,
                    expectedPayloadClass
            );
            throw new UnsupportedChecksumAlgorithmException(
                    buildReferenceContext(
                            context,
                            payloadClass,
                            reference,
                            "Unsupported checksum format checksum=" + expectedChecksum.orElseThrow()
                    )
            );
        }

        metrics.increment(
                "restore_binary_checksum_checks_total",
                context.restoreType(),
                configuredMessageType,
                expectedPayloadClass
        );
        String actualChecksum = normalizeChecksum(head.eTag())
                .filter(expectedChecksum.orElseThrow()::equals)
                .orElseGet(() -> streamMd5(bucketProperties, client, reference));
        if (!expectedChecksum.orElseThrow().equals(actualChecksum)) {
            metrics.increment(
                    "restore_binary_validation_failures_total",
                    context.restoreType(),
                    configuredMessageType,
                    expectedPayloadClass
            );
            throw new BinaryChecksumMismatchException(
                    buildReferenceContext(
                            context,
                            payloadClass,
                            reference,
                            "Binary checksum mismatch expectedChecksum="
                                    + expectedChecksum.orElseThrow()
                                    + " actualChecksum=" + actualChecksum
                    )
            );
        }
    }

    private String streamMd5(
            EngineS3Properties.BucketProperties bucketProperties,
            S3Client client,
            BinaryReference reference
    ) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            try (ResponseInputStream<?> inputStream = client.getObject(GetObjectRequest.builder()
                    .bucket(bucketProperties.getBucket())
                    .key(reference.objectKey())
                    .build())) {
                consume(new DigestInputStream(inputStream, digest));
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("MD5 checksum algorithm is unavailable", exception);
        } catch (IOException | S3Exception exception) {
            throw new BinaryVerificationException(
                    "Failed to stream binary for checksum verification fieldPath="
                            + reference.fieldPath()
                            + " objectKey=" + reference.objectKey(),
                    exception
            );
        }
    }

    private void consume(InputStream inputStream) throws IOException {
        byte[] buffer = new byte[8192];
        while (inputStream.read(buffer) != -1) {
            // Stream object bytes through the digest without buffering the whole object.
        }
    }

    private String buildReferenceContext(
            RestoreRecordValidationContext context,
            Class<?> payloadClass,
            BinaryReference reference,
            String detail
    ) {
        return detail
                + " restoreType=" + context.restoreType()
                + " sourceTopic=" + context.sourceTopic()
                + " partition=" + context.partition()
                + " offset=" + context.offset()
                + " expectedMessageType=" + payloadClass.getName()
                + " fieldPath=" + reference.fieldPath()
                + " bucketKey=" + reference.bucketKey()
                + " objectKey=" + reference.objectKey();
    }

    private Optional<String> normalizeChecksum(String checksum) {
        if (checksum == null) {
            return Optional.empty();
        }

        String normalized = checksum.trim().replace("\"", "").toLowerCase();
        if (normalized.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(normalized);
    }
}
