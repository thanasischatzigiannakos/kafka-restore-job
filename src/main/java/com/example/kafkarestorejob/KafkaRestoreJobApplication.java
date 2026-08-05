package com.example.kafkarestorejob;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Spring Boot entry point for the Kafka restore job application.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaRestoreJobApplication {

    /**
     * Starts the Spring Boot application.
     *
     * @param args standard application arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(KafkaRestoreJobApplication.class, args);
    }
}
