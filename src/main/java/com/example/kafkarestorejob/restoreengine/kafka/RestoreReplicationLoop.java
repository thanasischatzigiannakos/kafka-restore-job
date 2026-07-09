package com.example.kafkarestorejob.restoreengine.kafka;

import com.example.kafkarestorejob.restoreengine.config.EngineKafkaProperties;
import com.example.kafkarestorejob.restoreengine.config.KafkaClientConfiguration;
import com.example.kafkarestorejob.restoreengine.job.RestoreJobCancellationException;
import com.example.kafkarestorejob.restoreengine.serialization.RestoreMessageUnpackerResolver;
import com.example.kafkarestorejob.restoreengine.verification.BinaryVerificationService;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class RestoreReplicationLoop {

    private static final Logger log = LoggerFactory.getLogger(RestoreReplicationLoop.class);

    private final KafkaClientConfiguration kafkaClientConfiguration;
    private final RestoreTransformerResolver transformerResolver;
    private final KafkaOffsetCalculator offsetCalculator;
    private final RestoreMessageUnpackerResolver unpackerResolver;
    private final BinaryVerificationService binaryVerificationService;

    public RestoreReplicationLoop(
            KafkaClientConfiguration kafkaClientConfiguration,
            RestoreTransformerResolver transformerResolver,
            KafkaOffsetCalculator offsetCalculator,
            RestoreMessageUnpackerResolver unpackerResolver,
            BinaryVerificationService binaryVerificationService
    ) {
        this.kafkaClientConfiguration = kafkaClientConfiguration;
        this.transformerResolver = transformerResolver;
        this.offsetCalculator = offsetCalculator;
        this.unpackerResolver = unpackerResolver;
        this.binaryVerificationService = binaryVerificationService;
    }

    public RestoreExecutionResult restore(
            String restoreType,
            Instant restoreFromTimestamp,
            BooleanSupplier cancellationRequested
    ) {
        EngineKafkaProperties.PipelineProperties pipeline = kafkaClientConfiguration.requirePipeline(restoreType);
        RestoreTransformer transformer = transformerResolver.resolve(pipeline.getMessageType());
        EngineKafkaProperties engineKafkaProperties = kafkaClientConfiguration.getEngineKafkaProperties();

        int emptyPolls = 0;
        int committedBatches = 0;
        long restoredRecords = 0L;

        try (
                KafkaConsumer<String, byte[]> consumer = kafkaClientConfiguration.createConsumer(restoreType);
                KafkaProducer<String, byte[]> producer = kafkaClientConfiguration.createProducer(restoreType)
        ) {
            consumer.subscribe(List.of(pipeline.getSourceTopic()));
            seekToStartingOffsets(
                    consumer,
                    pipeline.getSourceTopic(),
                    restoreFromTimestamp,
                    engineKafkaProperties.getPollTimeout()
            );

            while (emptyPolls < engineKafkaProperties.getMaxEmptyPollsBeforeFinish()) {
                failIfCancellationRequested(cancellationRequested);
                ConsumerRecords<String, byte[]> records = consumer.poll(engineKafkaProperties.getPollTimeout());
                if (records.isEmpty()) {
                    emptyPolls++;
                    continue;
                }

                emptyPolls = 0;
                restoreBatch(
                        producer,
                        consumer,
                        records,
                        restoreType,
                        pipeline.getTargetTopic(),
                        pipeline.getMessageType(),
                        transformer,
                        cancellationRequested
                );
                committedBatches++;
                restoredRecords += records.count();
            }
        }

        log.info(
                "Restore finished for restoreType={} sourceTopic={} targetTopic={} restoreFromTimestamp={} batchesCommitted={} recordsRestored={}",
                restoreType,
                pipeline.getSourceTopic(),
                pipeline.getTargetTopic(),
                restoreFromTimestamp,
                committedBatches,
                restoredRecords
        );

        return new RestoreExecutionResult(
                restoreType,
                pipeline.getSourceTopic(),
                pipeline.getTargetTopic(),
                pipeline.getGroupId(),
                pipeline.getTransactionalId(),
                pipeline.getMessageType(),
                committedBatches,
                restoredRecords,
                emptyPolls
        );
    }

    private void restoreBatch(
            KafkaProducer<String, byte[]> producer,
            KafkaConsumer<String, byte[]> consumer,
            ConsumerRecords<String, byte[]> records,
            String restoreType,
            String targetTopic,
            String messageType,
            RestoreTransformer transformer,
            BooleanSupplier cancellationRequested
    ) {
        producer.beginTransaction();
        try {
            for (ConsumerRecord<String, byte[]> sourceRecord : records) {
                failIfCancellationRequested(cancellationRequested);
                Object payload = unpackerResolver.unpack(messageType, sourceRecord.value());
                binaryVerificationService.verify(restoreType, messageType, payload);
                producer.send(transformer.transform(targetTopic, sourceRecord)).get();
            }

            Map<TopicPartition, OffsetAndMetadata> offsets = offsetCalculator.calculateOffsets(records);
            producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
            producer.commitTransaction();
        } catch (InterruptedException exception) {
            abortTransactionSafely(producer, exception);
            Thread.currentThread().interrupt();
            throw new RestoreEngineException("Restore batch interrupted", exception);
        } catch (RestoreJobCancellationException exception) {
            abortTransactionSafely(producer, exception);
            throw exception;
        } catch (ExecutionException | RuntimeException exception) {
            abortTransactionSafely(producer, exception);
            throw new RestoreEngineException("Restore batch failed", exception);
        }
    }

    private void seekToStartingOffsets(
            KafkaConsumer<String, byte[]> consumer,
            String sourceTopic,
            Instant restoreFromTimestamp,
            Duration assignmentPollTimeout
    ) {
        Set<TopicPartition> assignment = awaitAssignment(consumer, assignmentPollTimeout);
        if (restoreFromTimestamp == null) {
            consumer.seekToBeginning(assignment);
            log.info("Restore starts at beginning for sourceTopic={}", sourceTopic);
            return;
        }

        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        for (TopicPartition topicPartition : assignment) {
            timestampsToSearch.put(topicPartition, restoreFromTimestamp.toEpochMilli());
        }

        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(timestampsToSearch);
        for (TopicPartition topicPartition : assignment) {
            OffsetAndTimestamp offsetAndTimestamp = offsetsForTimes.get(topicPartition);
            if (offsetAndTimestamp == null) {
                consumer.seekToBeginning(List.of(topicPartition));
                log.info(
                        "No offset exists at restoreFromTimestamp={} for sourceTopic={} partition={}; seeking to beginning",
                        restoreFromTimestamp,
                        sourceTopic,
                        topicPartition.partition()
                );
            } else {
                consumer.seek(topicPartition, offsetAndTimestamp.offset());
                log.info(
                        "Restore starts at offset={} for sourceTopic={} partition={} restoreFromTimestamp={}",
                        offsetAndTimestamp.offset(),
                        sourceTopic,
                        topicPartition.partition(),
                        restoreFromTimestamp
                );
            }
        }
    }

    private Set<TopicPartition> awaitAssignment(
            KafkaConsumer<String, byte[]> consumer,
            Duration assignmentPollTimeout
    ) {
        consumer.poll(Duration.ZERO);
        Set<TopicPartition> assignment = consumer.assignment();
        if (!assignment.isEmpty()) {
            return assignment;
        }

        consumer.poll(assignmentPollTimeout);
        assignment = consumer.assignment();
        if (assignment.isEmpty()) {
            throw new RestoreEngineException("Kafka consumer did not receive a partition assignment");
        }
        return assignment;
    }

    private void failIfCancellationRequested(BooleanSupplier cancellationRequested) {
        if (cancellationRequested.getAsBoolean()) {
            throw new RestoreJobCancellationException("Cancellation requested for running restore job");
        }
    }

    private void abortTransactionSafely(KafkaProducer<String, byte[]> producer, Exception originalException) {
        try {
            producer.abortTransaction();
        } catch (RuntimeException abortException) {
            originalException.addSuppressed(abortException);
        }
    }
}
