package com.example.kafkarestorejob.restoreengine.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import org.junit.jupiter.api.Test;

class EngineKafkaPropertiesTest {

    @Test
    void returnsAvailableRestoreTypesInConfiguredOrder() {
        EngineKafkaProperties properties = new EngineKafkaProperties();
        LinkedHashMap<String, EngineKafkaProperties.PipelineProperties> pipelines = new LinkedHashMap<>();
        pipelines.put("application", pipeline("source-a", "target-a"));
        pipelines.put("abuse", pipeline("source-b", "target-b"));
        properties.setPipelines(pipelines);

        assertIterableEquals(java.util.List.of("application", "abuse"), properties.availableRestoreTypes());
    }

    @Test
    void resolvesSourceAndTargetBootstrapServersWithFallback() {
        EngineKafkaProperties properties = new EngineKafkaProperties();
        properties.setBootstrapServers("shared:9092");

        assertEquals("shared:9092", properties.requireSourceBootstrapServers());
        assertEquals("shared:9092", properties.requireTargetBootstrapServers());

        properties.setSourceBootstrapServers("source:9092");
        properties.setTargetBootstrapServers("target:9092");

        assertEquals("source:9092", properties.requireSourceBootstrapServers());
        assertEquals("target:9092", properties.requireTargetBootstrapServers());
    }

    @Test
    void resolvesSourceAndTargetSecurityWithFallback() {
        EngineKafkaProperties properties = new EngineKafkaProperties();
        properties.setSecurityProtocol("SASL_SSL");
        properties.setSaslMechanism("SCRAM-SHA-512");
        properties.setTruststoreType("PKCS12");

        assertEquals("SASL_SSL", properties.resolveSourceSecurityProperties().getSecurityProtocol());
        assertEquals("SCRAM-SHA-512", properties.resolveTargetSecurityProperties().getSaslMechanism());

        EngineKafkaProperties.SecurityProperties source = new EngineKafkaProperties.SecurityProperties();
        source.setSecurityProtocol("SSL");
        source.setTruststoreType("JKS");
        properties.setSource(source);

        EngineKafkaProperties.SecurityProperties target = new EngineKafkaProperties.SecurityProperties();
        target.setSaslMechanism("PLAIN");
        properties.setTarget(target);

        assertEquals("SSL", properties.resolveSourceSecurityProperties().getSecurityProtocol());
        assertEquals("JKS", properties.resolveSourceSecurityProperties().getTruststoreType());
        assertEquals("PLAIN", properties.resolveTargetSecurityProperties().getSaslMechanism());
        assertEquals("SASL_SSL", properties.resolveTargetSecurityProperties().getSecurityProtocol());
    }

    @Test
    void requiresConfiguredPipeline() {
        EngineKafkaProperties properties = new EngineKafkaProperties();
        LinkedHashMap<String, EngineKafkaProperties.PipelineProperties> pipelines = new LinkedHashMap<>();
        EngineKafkaProperties.PipelineProperties pipeline = pipeline("source-a", "target-a");
        pipelines.put("application", pipeline);
        properties.setPipelines(pipelines);

        assertEquals(pipeline, properties.requirePipeline("application"));
        assertThrows(IllegalArgumentException.class, () -> properties.requirePipeline("missing"));
    }

    private EngineKafkaProperties.PipelineProperties pipeline(String sourceTopic, String targetTopic) {
        EngineKafkaProperties.PipelineProperties pipeline = new EngineKafkaProperties.PipelineProperties();
        pipeline.setSourceTopic(sourceTopic);
        pipeline.setTargetTopic(targetTopic);
        pipeline.setGroupId("group");
        pipeline.setTransactionalId("tx");
        return pipeline;
    }
}
