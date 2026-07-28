package com.example.kafkarestorejob.restoreengine.verification;

import com.example.kafkarestorejob.restoreengine.config.EngineS3Properties;
import com.example.kafkarestorejob.restoreengine.s3.S3ClientFactory;
import com.example.kafkarestorejob.restoreengine.validation.RestoreRecordValidationContext;
import com.example.kafkarestorejob.restoreengine.validation.RestorePayloadValidationException;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

@Component
public class S3FileExistenceVerifier {

    private final String bucket;
    private final S3Client client;

    public S3FileExistenceVerifier(
            EngineS3Properties properties,
            S3ClientFactory clientFactory
    ) {
        this.bucket = properties.getBucket();
        this.client = clientFactory.createClient();
    }

    public void verifyExists(RestoreRecordValidationContext context, String messageType, FileReference reference) {
        try {
            client.headObject(HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(reference.objectKey())
                    .build());
        } catch (S3Exception exception) {
            throw new RestorePayloadValidationException(
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
