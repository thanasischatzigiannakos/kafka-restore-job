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

@Service
public class KafkaMessagePreviewService {

    private final KafkaClientConfiguration kafkaClientConfiguration;

    public KafkaMessagePreviewService(KafkaClientConfiguration kafkaClientConfiguration) {
        this.kafkaClientConfiguration = kafkaClientConfiguration;
    }

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

    private Map<String, String> extractHeaders(ConsumerRecord<String, byte[]> record) {
        Map<String, String> headers = new LinkedHashMap<>();
        record.headers().forEach(header -> headers.put(header.key(), encodeBytes(header.value())));
        return headers;
    }

    private String encodeBytes(byte[] bytes) {
        return bytes == null ? "" : Base64.getEncoder().encodeToString(bytes);
    }
}
