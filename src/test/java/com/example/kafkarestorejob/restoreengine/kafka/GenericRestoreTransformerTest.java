package com.example.kafkarestorejob.restoreengine.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

class GenericRestoreTransformerTest {

    @Test
    void copiesPayloadAndAddsRestoreHeaders() {
        GenericRestoreTransformer transformer = new GenericRestoreTransformer();
        ConsumerRecord<String, byte[]> source = new ConsumerRecord<>(
                "source-topic",
                2,
                17L,
                "key",
                "value".getBytes(StandardCharsets.UTF_8)
        );
        source.headers().add("original", "header".getBytes(StandardCharsets.UTF_8));

        ProducerRecord<String, byte[]> target =
                transformer.transform("application", "target-topic", source);

        assertEquals("target-topic", target.topic());
        assertEquals(2, target.partition());
        assertEquals("key", target.key());
        assertArrayEquals("value".getBytes(StandardCharsets.UTF_8), target.value());
        assertEquals("header", new String(target.headers().lastHeader("original").value(), StandardCharsets.UTF_8));
        assertEquals("application", new String(target.headers().lastHeader("restore-message-type").value(), StandardCharsets.UTF_8));
        assertEquals("source-topic", new String(target.headers().lastHeader("restore-source-topic").value(), StandardCharsets.UTF_8));
        assertEquals("2", new String(target.headers().lastHeader("restore-source-partition").value(), StandardCharsets.UTF_8));
        assertEquals("17", new String(target.headers().lastHeader("restore-source-offset").value(), StandardCharsets.UTF_8));
    }
}
