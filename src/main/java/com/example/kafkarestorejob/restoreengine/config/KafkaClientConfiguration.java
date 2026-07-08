package com.example.kafkarestorejob.restoreengine.config;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

@Component
public class KafkaClientConfiguration {

    private final EngineKafkaProperties engineKafkaProperties;
    private final KafkaSecurityConfigHelper securityConfigHelper;

    public KafkaClientConfiguration(
            EngineKafkaProperties engineKafkaProperties,
            KafkaSecurityConfigHelper securityConfigHelper
    ) {
        this.engineKafkaProperties = engineKafkaProperties;
        this.securityConfigHelper = securityConfigHelper;
    }

    public EngineKafkaProperties.PipelineProperties requirePipeline(String restoreType) {
        return engineKafkaProperties.requirePipeline(restoreType);
    }

    public KafkaConsumer<String, byte[]> createConsumer(String restoreType) {
        EngineKafkaProperties.PipelineProperties pipeline = requirePipeline(restoreType);
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, engineKafkaProperties.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, pipeline.getGroupId());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, pipeline.getBatchSize());
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, Math.toIntExact(engineKafkaProperties.getRequestTimeout().toMillis()));
        securityConfigHelper.applySecurityProperties(config, engineKafkaProperties);
        return new KafkaConsumer<>(config);
    }

    public KafkaConsumer<String, byte[]> createPreviewConsumer(String previewConsumerId) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, engineKafkaProperties.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "preview-" + previewConsumerId);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        config.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, Math.toIntExact(engineKafkaProperties.getRequestTimeout().toMillis()));
        securityConfigHelper.applySecurityProperties(config, engineKafkaProperties);
        return new KafkaConsumer<>(config);
    }

    public KafkaProducer<String, byte[]> createProducer(String restoreType) {
        EngineKafkaProperties.PipelineProperties pipeline = requirePipeline(restoreType);
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, engineKafkaProperties.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, pipeline.getTransactionalId());
        config.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, Math.toIntExact(engineKafkaProperties.getTransactionTimeout().toMillis()));
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Math.toIntExact(engineKafkaProperties.getRequestTimeout().toMillis()));
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Math.toIntExact(engineKafkaProperties.getDeliveryTimeout().toMillis()));
        securityConfigHelper.applySecurityProperties(config, engineKafkaProperties);
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(config);
        producer.initTransactions();
        return producer;
    }

    public EngineKafkaProperties getEngineKafkaProperties() {
        return engineKafkaProperties;
    }
}
