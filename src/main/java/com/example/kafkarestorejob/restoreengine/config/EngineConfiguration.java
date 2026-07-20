package com.example.kafkarestorejob.restoreengine.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({
        EngineKafkaProperties.class,
        EngineS3Properties.class,
        EngineZookeeperProperties.class
})
public class EngineConfiguration {
}
