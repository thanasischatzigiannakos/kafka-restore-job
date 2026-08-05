package com.example.kafkarestorejob.restoreengine.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Binds S3 client settings used for binary validation.
 */
@Validated
@ConfigurationProperties(prefix = "engine.s3")
public class EngineS3Properties {

    @NotBlank
    private String bucket;

    private String endpoint;

    @NotBlank
    private String region;

    private String accessKey;

    private String secretKey;

    private boolean pathStyleAccess;

    @NotNull
    private Duration connectionTimeout;

    @NotNull
    private Duration readTimeout;

    /**
     * Returns the timeout used for establishing S3 connections.
     *
     * @return the connection timeout
     */
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * Sets the timeout used for establishing S3 connections.
     *
     * @param connectionTimeout the connection timeout
     */
    public void setConnectionTimeout(Duration connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    /**
     * Returns the timeout used for reading S3 responses.
     *
     * @return the read timeout
     */
    public Duration getReadTimeout() {
        return readTimeout;
    }

    /**
     * Sets the timeout used for reading S3 responses.
     *
     * @param readTimeout the read timeout
     */
    public void setReadTimeout(Duration readTimeout) {
        this.readTimeout = readTimeout;
    }

    /**
     * Returns the default bucket used for binary validation.
     *
     * @return the bucket name
     */
    public String getBucket() {
        return bucket;
    }

    /**
     * Sets the default bucket used for binary validation.
     *
     * @param bucket the bucket name
     */
    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    /**
     * Returns the optional custom S3 endpoint.
     *
     * @return the endpoint URL, or {@code null} to use the regional AWS endpoint
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * Sets the optional custom S3 endpoint.
     *
     * @param endpoint the endpoint URL
     */
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Returns the region used to build the S3 client.
     *
     * @return the S3 region
     */
    public String getRegion() {
        return region;
    }

    /**
     * Sets the region used to build the S3 client.
     *
     * @param region the S3 region
     */
    public void setRegion(String region) {
        this.region = region;
    }

    /**
     * Returns the optional access key used for static S3 credentials.
     *
     * @return the access key, or {@code null} when ambient credentials should be used
     */
    public String getAccessKey() {
        return accessKey;
    }

    /**
     * Sets the optional access key used for static S3 credentials.
     *
     * @param accessKey the access key
     */
    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    /**
     * Returns the optional secret key used for static S3 credentials.
     *
     * @return the secret key, or {@code null} when ambient credentials should be used
     */
    public String getSecretKey() {
        return secretKey;
    }

    /**
     * Sets the optional secret key used for static S3 credentials.
     *
     * @param secretKey the secret key
     */
    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    /**
     * Returns whether path-style S3 access is enabled.
     *
     * @return {@code true} when path-style access should be used
     */
    public boolean isPathStyleAccess() {
        return pathStyleAccess;
    }

    /**
     * Sets whether path-style S3 access is enabled.
     *
     * @param pathStyleAccess whether to use path-style access
     */
    public void setPathStyleAccess(boolean pathStyleAccess) {
        this.pathStyleAccess = pathStyleAccess;
    }
}
