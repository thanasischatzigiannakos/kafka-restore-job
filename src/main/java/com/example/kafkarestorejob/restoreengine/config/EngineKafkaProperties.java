package com.example.kafkarestorejob.restoreengine.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "engine.kafka")
public class EngineKafkaProperties {

    @NotBlank
    private String bootstrapServers;

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

    public PipelineProperties requirePipeline(String restoreType) {
        PipelineProperties pipelineProperties = pipelines.get(restoreType);
        if (pipelineProperties == null) {
            throw new IllegalArgumentException("No Kafka pipeline configured for restore type: " + restoreType);
        }
        return pipelineProperties;
    }

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
}
