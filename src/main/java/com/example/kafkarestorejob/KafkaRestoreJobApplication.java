package com.example.kafkarestorejob;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaRestoreJobApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaRestoreJobApplication.class, args);
    }
}
