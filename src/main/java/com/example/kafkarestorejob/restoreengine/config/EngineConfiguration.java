package com.example.kafkarestorejob.restoreengine.config;

import jakarta.validation.constraints.NotEmpty;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

@Configuration
@EnableConfigurationProperties({
        EngineKafkaProperties.class,
        EngineS3Properties.class,
        EngineZookeeperProperties.class,
        EngineConfiguration.VerificationProperties.class
})
public class EngineConfiguration {

    @Validated
    @ConfigurationProperties(prefix = "engine.verification")
    public static class VerificationProperties {

        @NotEmpty
        private Map<String, String> typeMappings = new LinkedHashMap<>();

        public Map<String, String> getTypeMappings() {
            return typeMappings;
        }

        public void setTypeMappings(Map<String, String> typeMappings) {
            this.typeMappings = typeMappings;
        }
    }
}
