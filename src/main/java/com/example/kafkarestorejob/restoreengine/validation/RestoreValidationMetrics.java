package com.example.kafkarestorejob.restoreengine.validation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.DoubleAdder;
import org.springframework.stereotype.Component;

@Component
public class RestoreValidationMetrics {

    private final Map<String, DoubleAdder> counters = new ConcurrentHashMap<>();

    public void increment(String metricName, String restoreType, String messageType, String expectedPayloadClass) {
        increment(metricName, restoreType, messageType, expectedPayloadClass, 1.0d);
    }

    public void increment(
            String metricName,
            String restoreType,
            String messageType,
            String expectedPayloadClass,
            double amount
    ) {
        counter(metricName, restoreType, messageType, expectedPayloadClass).add(amount);
    }

    public double count(String metricName, String restoreType, String messageType, String expectedPayloadClass) {
        return counter(metricName, restoreType, messageType, expectedPayloadClass).sum();
    }

    private DoubleAdder counter(
            String metricName,
            String restoreType,
            String messageType,
            String expectedPayloadClass
    ) {
        return counters.computeIfAbsent(
                metricName + "|" + restoreType + "|" + messageType + "|" + expectedPayloadClass,
                ignored -> new DoubleAdder()
        );
    }
}
