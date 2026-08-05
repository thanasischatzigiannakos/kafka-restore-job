package com.example.kafkarestorejob.restoreengine.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;

class KafkaSecurityConfigHelperTest {

    @Test
    void appliesConfiguredSecurityPropertiesAndSkipsBlankValues() {
        KafkaSecurityConfigHelper helper = new KafkaSecurityConfigHelper();
        EngineKafkaProperties.SecurityProperties security = new EngineKafkaProperties.SecurityProperties();
        security.setSecurityProtocol("SASL_SSL");
        security.setSaslMechanism("SCRAM-SHA-512");
        security.setSaslJaasConfig("loginModule");
        security.setTruststoreLocation("/etc/truststore.p12");
        security.setTruststorePassword("secret");
        security.setTruststoreType("PKCS12");
        security.setKeystoreLocation(" ");
        security.setKeystorePassword("");
        security.setKeyPassword(null);
        security.setKeystoreType("PKCS12");

        Map<String, Object> config = new HashMap<>();
        helper.applySecurityProperties(config, security);

        assertEquals("SASL_SSL", config.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("SCRAM-SHA-512", config.get(SaslConfigs.SASL_MECHANISM));
        assertEquals("loginModule", config.get(SaslConfigs.SASL_JAAS_CONFIG));
        assertEquals("/etc/truststore.p12", config.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        assertEquals("secret", config.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        assertEquals("PKCS12", config.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));
        assertEquals("PKCS12", config.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG));
        assertFalse(config.containsKey(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        assertFalse(config.containsKey(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
        assertFalse(config.containsKey(SslConfigs.SSL_KEY_PASSWORD_CONFIG));
    }
}
