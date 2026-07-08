package com.example.kafkarestorejob.restoreengine.verification;

import com.example.kafkarestorejob.restoreengine.config.EngineConfiguration;
import com.example.kafkarestorejob.restoreengine.config.EngineS3Properties;
import com.example.kafkarestorejob.restoreengine.s3.S3ClientFactory;
import com.example.kafkarestorejob.restoreengine.s3.S3BucketResolver;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

@Service
public class BinaryVerificationService {

    private final EngineConfiguration.VerificationProperties verificationProperties;
    private final BinaryReferenceExtractorResolver extractorResolver;
    private final S3BucketResolver bucketResolver;
    private final S3ClientFactory clientFactory;

    public BinaryVerificationService(
            EngineConfiguration.VerificationProperties verificationProperties,
            BinaryReferenceExtractorResolver extractorResolver,
            S3BucketResolver bucketResolver,
            S3ClientFactory clientFactory
    ) {
        this.verificationProperties = verificationProperties;
        this.extractorResolver = extractorResolver;
        this.bucketResolver = bucketResolver;
        this.clientFactory = clientFactory;
    }

    public void verify(String restoreType, String messageType, Object payload) {
        String verificationType = verificationProperties.getTypeMappings().get(restoreType);
        if (verificationType == null) {
            throw new IllegalArgumentException("No verification mapping configured for restore type: " + restoreType);
        }

        List<BinaryReference> references = extractorResolver.resolve(messageType).extractUntyped(payload);
        Map<String, S3Client> clientsByBucketKey = new HashMap<>();

        try {
            for (BinaryReference reference : references) {
                String bucketKey = reference.bucketKey();
                EngineS3Properties.BucketProperties bucketProperties = bucketResolver.resolve(bucketKey);
                S3Client client = clientsByBucketKey.computeIfAbsent(
                        normalizeBucketKey(bucketKey),
                        ignored -> clientFactory.createClient(bucketKey)
                );
                verifyReference(verificationType, bucketProperties, client, reference);
            }
        } finally {
            clientsByBucketKey.values().forEach(S3Client::close);
        }
    }

    private void verifyReference(
            String verificationType,
            EngineS3Properties.BucketProperties bucketProperties,
            S3Client client,
            BinaryReference reference
    ) {
        try {
            client.headObject(HeadObjectRequest.builder()
                    .bucket(bucketProperties.getBucket())
                    .key(reference.objectKey())
                    .build());
        } catch (S3Exception exception) {
            throw new BinaryVerificationException(
                    "Missing binary for verificationType=" + verificationType
                            + " fieldPath=" + reference.fieldPath()
                            + " bucket=" + bucketProperties.getBucket()
                            + " key=" + reference.objectKey(),
                    exception
            );
        }
    }

    private String normalizeBucketKey(String bucketKey) {
        return (bucketKey == null || bucketKey.isBlank()) ? "__default__" : bucketKey;
    }
}
