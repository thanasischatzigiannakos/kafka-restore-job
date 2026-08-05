package com.example.kafkarestorejob.restoreengine.service;

import com.example.kafkarestorejob.restoreengine.api.KafkaMessagePreviewResponse;
import com.example.kafkarestorejob.restoreengine.config.EngineKafkaProperties;
import com.example.kafkarestorejob.restoreengine.config.KafkaClientConfiguration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

/**
 * Reads a bounded preview of records from a configured source or target topic.
 */
@Service
public class KafkaMessagePreviewService {

    private final KafkaClientConfiguration kafkaClientConfiguration;

    /**
     * Creates the service with the Kafka client configuration used to build preview consumers.
     *
     * @param kafkaClientConfiguration the Kafka client configuration
     */
    public KafkaMessagePreviewService(KafkaClientConfiguration kafkaClientConfiguration) {
        this.kafkaClientConfiguration = kafkaClientConfiguration;
    }

    /**
     * Returns a bounded preview of records from the selected topic of a configured pipeline.
     *
     * @param restoreType the logical restore type whose pipeline should be previewed
     * @param topicSelector the topic selector, typically {@code source} or {@code target}
     * @param limit the maximum number of records to preview
     * @return the preview response
     */
    public KafkaMessagePreviewResponse preview(String restoreType, String topicSelector, int limit) {
        EngineKafkaProperties.PipelineProperties pipeline = kafkaClientConfiguration.requirePipeline(restoreType);
        String topic = "target".equalsIgnoreCase(topicSelector) ? pipeline.getTargetTopic() : pipeline.getSourceTopic();
        int boundedLimit = Math.max(1, Math.min(limit, 50));

        try (KafkaConsumer<String, byte[]> consumer = kafkaClientConfiguration.createPreviewConsumer(UUID.randomUUID().toString())) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            List<TopicPartition> partitions = partitionInfos.stream()
                    .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
                    .toList();

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            List<KafkaMessagePreviewResponse.KafkaPreviewMessage> messages = new ArrayList<>();
            while (messages.size() < boundedLimit) {
                ConsumerRecords<String, byte[]> records = consumer.poll(kafkaClientConfiguration.getEngineKafkaProperties().getPollTimeout());
                if (records.isEmpty()) {
                    break;
                }

                for (ConsumerRecord<String, byte[]> record : records) {
                    messages.add(new KafkaMessagePreviewResponse.KafkaPreviewMessage(
                            record.partition(),
                            record.offset(),
                            record.key(),
                            encodeBytes(record.value()),
                            extractHeaders(record)
                    ));
                    if (messages.size() >= boundedLimit) {
                        break;
                    }
                }
            }

            return new KafkaMessagePreviewResponse(restoreType, topic, messages.size(), messages);
        }
    }

    /**
     * Extracts and Base64-encodes the headers of a Kafka record.
     *
     * @param record the Kafka record
     * @return the encoded headers keyed by header name
     */
    private Map<String, String> extractHeaders(ConsumerRecord<String, byte[]> record) {
        Map<String, String> headers = new LinkedHashMap<>();
        record.headers().forEach(header -> headers.put(header.key(), encodeBytes(header.value())));
        return headers;
    }

    /**
     * Encodes the supplied bytes as Base64 for API transport.
     *
     * @param bytes the bytes to encode
     * @return the Base64-encoded value, or an empty string when the input is {@code null}
     */
    private String encodeBytes(byte[] bytes) {
        return bytes == null ? "" : Base64.getEncoder().encodeToString(bytes);
    }
}
