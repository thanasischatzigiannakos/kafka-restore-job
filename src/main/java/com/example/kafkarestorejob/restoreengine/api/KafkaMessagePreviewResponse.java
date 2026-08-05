package com.example.kafkarestorejob.restoreengine.api;

import java.util.List;
import java.util.Map;

/**
 * API response describing a bounded preview of messages from a configured source or target topic.
 *
 * @param restoreType the logical restore type whose pipeline was previewed
 * @param topic the concrete topic that was read
 * @param messageCount the number of previewed messages returned
 * @param messages the previewed messages in read order
 */
public record KafkaMessagePreviewResponse(
        String restoreType,
        String topic,
        int messageCount,
        List<KafkaPreviewMessage> messages
) {
    /**
     * One previewed Kafka record encoded for API transport.
     *
     * @param partition the partition containing the record
     * @param offset the offset of the record within the partition
     * @param key the record key
     * @param payloadBase64 the record payload encoded as Base64
     * @param headers the record headers encoded as Base64 values by header name
     */
    public record KafkaPreviewMessage(
            int partition,
            long offset,
            String key,
            String payloadBase64,
            Map<String, String> headers
    ) {
    }
}
