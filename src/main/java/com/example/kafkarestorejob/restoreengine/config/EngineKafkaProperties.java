package com.example.kafkarestorejob.restoreengine.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Binds Kafka client settings for the restore engine.
 *
 * <p>The root properties provide shared defaults for both Kafka clusters, while the optional
 * {@code source} and {@code target} sections override security settings for cross-cluster
 * deployments. Individual restore pipelines are keyed by restore type.
 */
@Validated
@ConfigurationProperties(prefix = "engine.kafka")
public class EngineKafkaProperties {

    @NotBlank
    private String bootstrapServers;

    private String sourceBootstrapServers;

    private String targetBootstrapServers;

    @Valid
    private SecurityProperties source = new SecurityProperties();

    @Valid
    private SecurityProperties target = new SecurityProperties();

    @NotNull
    private Duration pollTimeout;

    @NotNull
    private Duration transactionTimeout;

    @NotNull
    private Duration requestTimeout;

    @NotNull
    private Duration deliveryTimeout;

    @Min(1)
    private int maxEmptyPollsBeforeFinish;

    @NotBlank
    private String securityProtocol = "PLAINTEXT";

    private String saslMechanism;

    private String saslJaasConfig;

    private String truststoreLocation;

    private String truststorePassword;

    private String truststoreType = "PKCS12";

    private String keystoreLocation;

    private String keystorePassword;

    private String keyPassword;

    private String keystoreType = "PKCS12";

    @Valid
    @NotEmpty
    private Map<String, PipelineProperties> pipelines = new LinkedHashMap<>();

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getSourceBootstrapServers() {
        return sourceBootstrapServers;
    }

    public void setSourceBootstrapServers(String sourceBootstrapServers) {
        this.sourceBootstrapServers = sourceBootstrapServers;
    }

    public String getTargetBootstrapServers() {
        return targetBootstrapServers;
    }

    public void setTargetBootstrapServers(String targetBootstrapServers) {
        this.targetBootstrapServers = targetBootstrapServers;
    }

    /**
     * Returns the bootstrap servers used by source consumers.
     *
     * @return the source-specific bootstrap servers, or the shared bootstrap servers when no
     *         source override is configured
     */
    public String requireSourceBootstrapServers() {
        return hasText(sourceBootstrapServers) ? sourceBootstrapServers : bootstrapServers;
    }

    /**
     * Returns the bootstrap servers used by target producers.
     *
     * @return the target-specific bootstrap servers, or the shared bootstrap servers when no
     *         target override is configured
     */
    public String requireTargetBootstrapServers() {
        return hasText(targetBootstrapServers) ? targetBootstrapServers : bootstrapServers;
    }

    public Duration getPollTimeout() {
        return pollTimeout;
    }

    public void setPollTimeout(Duration pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    public Duration getTransactionTimeout() {
        return transactionTimeout;
    }

    public void setTransactionTimeout(Duration transactionTimeout) {
        this.transactionTimeout = transactionTimeout;
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(Duration requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    public Duration getDeliveryTimeout() {
        return deliveryTimeout;
    }

    public void setDeliveryTimeout(Duration deliveryTimeout) {
        this.deliveryTimeout = deliveryTimeout;
    }

    public int getMaxEmptyPollsBeforeFinish() {
        return maxEmptyPollsBeforeFinish;
    }

    public void setMaxEmptyPollsBeforeFinish(int maxEmptyPollsBeforeFinish) {
        this.maxEmptyPollsBeforeFinish = maxEmptyPollsBeforeFinish;
    }

    public SecurityProperties getSource() {
        return source;
    }

    public void setSource(SecurityProperties source) {
        this.source = source;
    }

    public SecurityProperties getTarget() {
        return target;
    }

    public void setTarget(SecurityProperties target) {
        this.target = target;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    public String getSaslJaasConfig() {
        return saslJaasConfig;
    }

    public void setSaslJaasConfig(String saslJaasConfig) {
        this.saslJaasConfig = saslJaasConfig;
    }

    public String getTruststoreLocation() {
        return truststoreLocation;
    }

    public void setTruststoreLocation(String truststoreLocation) {
        this.truststoreLocation = truststoreLocation;
    }

    public String getTruststorePassword() {
        return truststorePassword;
    }

    public void setTruststorePassword(String truststorePassword) {
        this.truststorePassword = truststorePassword;
    }

    public String getTruststoreType() {
        return truststoreType;
    }

    public void setTruststoreType(String truststoreType) {
        this.truststoreType = truststoreType;
    }

    public String getKeystoreLocation() {
        return keystoreLocation;
    }

    public void setKeystoreLocation(String keystoreLocation) {
        this.keystoreLocation = keystoreLocation;
    }

    public String getKeystorePassword() {
        return keystorePassword;
    }

    public void setKeystorePassword(String keystorePassword) {
        this.keystorePassword = keystorePassword;
    }

    public String getKeyPassword() {
        return keyPassword;
    }

    public void setKeyPassword(String keyPassword) {
        this.keyPassword = keyPassword;
    }

    public String getKeystoreType() {
        return keystoreType;
    }

    public void setKeystoreType(String keystoreType) {
        this.keystoreType = keystoreType;
    }

    public Map<String, PipelineProperties> getPipelines() {
        return pipelines;
    }

    public void setPipelines(Map<String, PipelineProperties> pipelines) {
        this.pipelines = pipelines;
    }

    /**
     * Resolves the configured pipeline for a restore type.
     *
     * @param restoreType the logical restore type key
     * @return the configured pipeline
     * @throws IllegalArgumentException when the type is not configured
     */
    public PipelineProperties requirePipeline(String restoreType) {
        PipelineProperties pipelineProperties = pipelines.get(restoreType);
        if (pipelineProperties == null) {
            throw new IllegalArgumentException("No Kafka pipeline configured for restore type: " + restoreType);
        }
        return pipelineProperties;
    }

    /**
     * Lists all configured restore types in declaration order.
     *
     * @return the configured restore-type keys
     */
    public List<String> availableRestoreTypes() {
        return List.copyOf(pipelines.keySet());
    }

    /**
     * Resolves effective source-side Kafka security settings by overlaying source-specific values
     * on top of the shared defaults.
     *
     * @return the effective source security settings
     */
    public SecurityProperties resolveSourceSecurityProperties() {
        return SecurityProperties.withFallback(source, this);
    }

    /**
     * Resolves effective target-side Kafka security settings by overlaying target-specific values
     * on top of the shared defaults.
     *
     * @return the effective target security settings
     */
    public SecurityProperties resolveTargetSecurityProperties() {
        return SecurityProperties.withFallback(target, this);
    }

    /**
     * Returns whether the provided value contains non-whitespace text.
     *
     * @param value the value to inspect
     * @return {@code true} when the value contains at least one non-whitespace character
     */
    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    /**
     * Holds the topic and client identities for one logical restore pipeline.
     */
    public static class PipelineProperties {

        @NotBlank
        private String sourceTopic;

        @NotBlank
        private String targetTopic;

        @NotBlank
        private String groupId;

        @NotBlank
        private String transactionalId;

        public String getSourceTopic() {
            return sourceTopic;
        }

        public void setSourceTopic(String sourceTopic) {
            this.sourceTopic = sourceTopic;
        }

        public String getTargetTopic() {
            return targetTopic;
        }

        public void setTargetTopic(String targetTopic) {
            this.targetTopic = targetTopic;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public String getTransactionalId() {
            return transactionalId;
        }

        public void setTransactionalId(String transactionalId) {
            this.transactionalId = transactionalId;
        }
    }

    /**
     * Holds one side's Kafka security settings and can merge them with the shared defaults.
     */
    public static class SecurityProperties {

        private String securityProtocol;

        private String saslMechanism;

        private String saslJaasConfig;

        private String truststoreLocation;

        private String truststorePassword;

        private String truststoreType;

        private String keystoreLocation;

        private String keystorePassword;

        private String keyPassword;

        private String keystoreType;

        public String getSecurityProtocol() {
            return securityProtocol;
        }

        public void setSecurityProtocol(String securityProtocol) {
            this.securityProtocol = securityProtocol;
        }

        public String getSaslMechanism() {
            return saslMechanism;
        }

        public void setSaslMechanism(String saslMechanism) {
            this.saslMechanism = saslMechanism;
        }

        public String getSaslJaasConfig() {
            return saslJaasConfig;
        }

        public void setSaslJaasConfig(String saslJaasConfig) {
            this.saslJaasConfig = saslJaasConfig;
        }

        public String getTruststoreLocation() {
            return truststoreLocation;
        }

        public void setTruststoreLocation(String truststoreLocation) {
            this.truststoreLocation = truststoreLocation;
        }

        public String getTruststorePassword() {
            return truststorePassword;
        }

        public void setTruststorePassword(String truststorePassword) {
            this.truststorePassword = truststorePassword;
        }

        public String getTruststoreType() {
            return truststoreType;
        }

        public void setTruststoreType(String truststoreType) {
            this.truststoreType = truststoreType;
        }

        public String getKeystoreLocation() {
            return keystoreLocation;
        }

        public void setKeystoreLocation(String keystoreLocation) {
            this.keystoreLocation = keystoreLocation;
        }

        public String getKeystorePassword() {
            return keystorePassword;
        }

        public void setKeystorePassword(String keystorePassword) {
            this.keystorePassword = keystorePassword;
        }

        public String getKeyPassword() {
            return keyPassword;
        }

        public void setKeyPassword(String keyPassword) {
            this.keyPassword = keyPassword;
        }

        public String getKeystoreType() {
            return keystoreType;
        }

        public void setKeystoreType(String keystoreType) {
            this.keystoreType = keystoreType;
        }

        /**
         * Merges the side-specific overrides with the shared root properties.
         *
         * @param override the side-specific override block
         * @param fallback the shared root properties
         * @return the effective security settings
         */
        private static SecurityProperties withFallback(
                SecurityProperties override,
                EngineKafkaProperties fallback
        ) {
            SecurityProperties resolved = new SecurityProperties();
            resolved.setSecurityProtocol(firstNonBlank(
                    override == null ? null : override.getSecurityProtocol(),
                    fallback.getSecurityProtocol()
            ));
            resolved.setSaslMechanism(firstNonBlank(
                    override == null ? null : override.getSaslMechanism(),
                    fallback.getSaslMechanism()
            ));
            resolved.setSaslJaasConfig(firstNonBlank(
                    override == null ? null : override.getSaslJaasConfig(),
                    fallback.getSaslJaasConfig()
            ));
            resolved.setTruststoreLocation(firstNonBlank(
                    override == null ? null : override.getTruststoreLocation(),
                    fallback.getTruststoreLocation()
            ));
            resolved.setTruststorePassword(firstNonBlank(
                    override == null ? null : override.getTruststorePassword(),
                    fallback.getTruststorePassword()
            ));
            resolved.setTruststoreType(firstNonBlank(
                    override == null ? null : override.getTruststoreType(),
                    fallback.getTruststoreType()
            ));
            resolved.setKeystoreLocation(firstNonBlank(
                    override == null ? null : override.getKeystoreLocation(),
                    fallback.getKeystoreLocation()
            ));
            resolved.setKeystorePassword(firstNonBlank(
                    override == null ? null : override.getKeystorePassword(),
                    fallback.getKeystorePassword()
            ));
            resolved.setKeyPassword(firstNonBlank(
                    override == null ? null : override.getKeyPassword(),
                    fallback.getKeyPassword()
            ));
            resolved.setKeystoreType(firstNonBlank(
                    override == null ? null : override.getKeystoreType(),
                    fallback.getKeystoreType()
            ));
            return resolved;
        }

        /**
         * Picks the preferred value when present, otherwise falls back to the shared one.
         *
         * @param preferred the side-specific value
         * @param fallback the shared fallback value
         * @return the resolved value
         */
        private static String firstNonBlank(String preferred, String fallback) {
            if (preferred != null && !preferred.isBlank()) {
                return preferred;
            }
            return fallback;
        }
    }
}
