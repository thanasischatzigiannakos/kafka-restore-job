package com.example.kafkarestorejob.restoreengine.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "engine.s3")
public class EngineS3Properties {

    @NotBlank
    private String defaultBucketKey;

    @NotNull
    private Duration connectionTimeout;

    @NotNull
    private Duration readTimeout;

    @Valid
    @NotEmpty
    private Map<String, BucketProperties> buckets = new LinkedHashMap<>();

    public String getDefaultBucketKey() {
        return defaultBucketKey;
    }

    public void setDefaultBucketKey(String defaultBucketKey) {
        this.defaultBucketKey = defaultBucketKey;
    }

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(Duration connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public Duration getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(Duration readTimeout) {
        this.readTimeout = readTimeout;
    }

    public Map<String, BucketProperties> getBuckets() {
        return buckets;
    }

    public void setBuckets(Map<String, BucketProperties> buckets) {
        this.buckets = buckets;
    }

    public static class BucketProperties {

        @NotBlank
        private String bucket;

        private String endpoint;

        @NotBlank
        private String region;

        private String accessKey;

        private String secretKey;

        private boolean pathStyleAccess;

        public String getBucket() {
            return bucket;
        }

        public void setBucket(String bucket) {
            this.bucket = bucket;
        }

        public String getEndpoint() {
            return endpoint;
        }

        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }

        public String getRegion() {
            return region;
        }

        public void setRegion(String region) {
            this.region = region;
        }

        public String getAccessKey() {
            return accessKey;
        }

        public void setAccessKey(String accessKey) {
            this.accessKey = accessKey;
        }

        public String getSecretKey() {
            return secretKey;
        }

        public void setSecretKey(String secretKey) {
            this.secretKey = secretKey;
        }

        public boolean isPathStyleAccess() {
            return pathStyleAccess;
        }

        public void setPathStyleAccess(boolean pathStyleAccess) {
            this.pathStyleAccess = pathStyleAccess;
        }
    }
}
