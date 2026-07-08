package com.example.kafkarestorejob.restoreengine.s3;

import com.example.kafkarestorejob.restoreengine.config.EngineS3Properties;
import java.net.URI;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

@Component
public class S3ClientFactory {

    private final EngineS3Properties properties;
    private final S3BucketResolver bucketResolver;

    public S3ClientFactory(EngineS3Properties properties, S3BucketResolver bucketResolver) {
        this.properties = properties;
        this.bucketResolver = bucketResolver;
    }

    public S3Client createClient(String bucketKey) {
        EngineS3Properties.BucketProperties bucketProperties = bucketResolver.resolve(bucketKey);

        S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(bucketProperties.getRegion()))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(bucketProperties.isPathStyleAccess()).build())
                .overrideConfiguration(
                        ClientOverrideConfiguration.builder()
                                .apiCallAttemptTimeout(properties.getReadTimeout())
                                .apiCallTimeout(properties.getConnectionTimeout())
                                .build()
                );

        if (StringUtils.hasText(bucketProperties.getEndpoint())) {
            builder.endpointOverride(URI.create(bucketProperties.getEndpoint()));
        }

        if (StringUtils.hasText(bucketProperties.getAccessKey()) && StringUtils.hasText(bucketProperties.getSecretKey())) {
            builder.credentialsProvider(
                    StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(bucketProperties.getAccessKey(), bucketProperties.getSecretKey())
                    )
            );
        }

        return builder.build();
    }
}
