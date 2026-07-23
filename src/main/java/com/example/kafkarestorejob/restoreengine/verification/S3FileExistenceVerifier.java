package com.example.kafkarestorejob.restoreengine.verification;

import com.example.kafkarestorejob.restoreengine.config.EngineS3Properties;
import com.example.kafkarestorejob.restoreengine.s3.S3BucketResolver;
import com.example.kafkarestorejob.restoreengine.s3.S3ClientFactory;
import com.example.kafkarestorejob.restoreengine.validation.RestoreRecordValidationContext;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

@Component
public class S3FileExistenceVerifier {

    private final EngineS3Properties.BucketProperties bucketProperties;
    private final S3Client client;

    public S3FileExistenceVerifier(
            EngineS3Properties properties,
            S3BucketResolver bucketResolver,
            S3ClientFactory clientFactory
    ) {
        this.bucketProperties = bucketResolver.resolve(properties.getDefaultBucketKey());
        this.client = clientFactory.createClient(properties.getDefaultBucketKey());
    }

    public void verifyExists(RestoreRecordValidationContext context, String messageType, FileReference reference) {
        try {
            client.headObject(HeadObjectRequest.builder()
                    .bucket(bucketProperties.getBucket())
                    .key(reference.objectKey())
                    .build());
        } catch (S3Exception exception) {
            throw new BinaryVerificationException(
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
    }
}
