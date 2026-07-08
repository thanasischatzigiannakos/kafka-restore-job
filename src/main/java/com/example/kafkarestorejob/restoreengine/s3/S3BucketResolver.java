package com.example.kafkarestorejob.restoreengine.s3;

import com.example.kafkarestorejob.restoreengine.config.EngineS3Properties;
import org.springframework.stereotype.Component;

@Component
public class S3BucketResolver {

    private final EngineS3Properties properties;

    public S3BucketResolver(EngineS3Properties properties) {
        this.properties = properties;
    }

    public EngineS3Properties.BucketProperties resolve(String bucketKey) {
        String resolvedKey = (bucketKey == null || bucketKey.isBlank()) ? properties.getDefaultBucketKey() : bucketKey;
        EngineS3Properties.BucketProperties bucketProperties = properties.getBuckets().get(resolvedKey);
        if (bucketProperties == null) {
            throw new IllegalArgumentException("No S3 bucket configured for key: " + resolvedKey);
        }
        return bucketProperties;
    }
}
