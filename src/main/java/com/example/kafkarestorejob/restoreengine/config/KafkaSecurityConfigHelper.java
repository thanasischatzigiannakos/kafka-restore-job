package com.example.kafkarestorejob.restoreengine.config;

import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * Applies resolved Kafka security settings to consumer or producer configuration maps.
 */
@Component
public class KafkaSecurityConfigHelper {

    /**
     * Copies all configured security values into the provided Kafka client config map.
     *
     * @param config the target config map
     * @param properties the resolved security properties to apply
     */
    public void applySecurityProperties(
            Map<String, Object> config,
            EngineKafkaProperties.SecurityProperties properties
    ) {
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, properties.getSecurityProtocol());
        putIfHasText(config, SaslConfigs.SASL_MECHANISM, properties.getSaslMechanism());
        putIfHasText(config, SaslConfigs.SASL_JAAS_CONFIG, properties.getSaslJaasConfig());
        putIfHasText(config, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, properties.getTruststoreLocation());
        putIfHasText(config, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, properties.getTruststorePassword());
        putIfHasText(config, SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, properties.getTruststoreType());
        putIfHasText(config, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, properties.getKeystoreLocation());
        putIfHasText(config, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, properties.getKeystorePassword());
        putIfHasText(config, SslConfigs.SSL_KEY_PASSWORD_CONFIG, properties.getKeyPassword());
        putIfHasText(config, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, properties.getKeystoreType());
    }

    /**
     * Adds a config entry only when the value contains non-blank text.
     *
     * @param config the target config map
     * @param key the Kafka config key
     * @param value the candidate config value
     */
    private void putIfHasText(Map<String, Object> config, String key, String value) {
        if (StringUtils.hasText(value)) {
            config.put(key, value);
        }
    }
}
