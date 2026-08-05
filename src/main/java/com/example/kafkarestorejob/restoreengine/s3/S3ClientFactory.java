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

/**
 * Builds S3 clients using the configured restore-engine S3 properties.
 */
@Component
public class S3ClientFactory {

    private final EngineS3Properties properties;

    /**
     * Creates the factory with the bound S3 properties.
     *
     * @param properties the S3 properties
     */
    public S3ClientFactory(EngineS3Properties properties) {
        this.properties = properties;
    }

    /**
     * Creates an S3 client configured for the current environment and optional custom endpoint.
     *
     * @return the configured S3 client
     */
    public S3Client createClient() {
        S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(properties.getRegion()))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(properties.isPathStyleAccess()).build())
                .overrideConfiguration(
                        ClientOverrideConfiguration.builder()
                                .apiCallAttemptTimeout(properties.getReadTimeout())
                                .apiCallTimeout(properties.getConnectionTimeout())
                                .build()
                );

        if (StringUtils.hasText(properties.getEndpoint())) {
            builder.endpointOverride(URI.create(properties.getEndpoint()));
        }

        if (StringUtils.hasText(properties.getAccessKey()) && StringUtils.hasText(properties.getSecretKey())) {
            builder.credentialsProvider(
                    StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(properties.getAccessKey(), properties.getSecretKey())
                    )
            );
        }

        return builder.build();
    }
}
