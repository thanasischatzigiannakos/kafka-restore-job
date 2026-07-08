package com.example.kafkarestorejob.restoreengine.api;

import java.util.List;
import java.util.Map;

public record KafkaMessagePreviewResponse(
        String restoreType,
        String topic,
        int messageCount,
        List<KafkaPreviewMessage> messages
) {
    public record KafkaPreviewMessage(
            int partition,
            long offset,
            String key,
            String payloadBase64,
            Map<String, String> headers
    ) {
    }
}
