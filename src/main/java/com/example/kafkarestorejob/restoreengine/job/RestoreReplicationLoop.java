package com.example.kafkarestorejob.restoreengine.job;

import com.example.kafkarestorejob.restoreengine.config.EngineKafkaProperties;
import com.example.kafkarestorejob.restoreengine.config.KafkaClientConfiguration;
import com.example.kafkarestorejob.restoreengine.kafka.KafkaOffsetCalculator;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreEngineException;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreExecutionResult;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class RestoreReplicationLoop {

    private static final Logger log = LoggerFactory.getLogger(RestoreReplicationLoop.class);

    private final KafkaClientConfiguration kafkaClientConfiguration;
    private final KafkaOffsetCalculator offsetCalculator;

    public RestoreReplicationLoop(
            KafkaClientConfiguration kafkaClientConfiguration,
            KafkaOffsetCalculator offsetCalculator
    ) {
        this.kafkaClientConfiguration = kafkaClientConfiguration;
        this.offsetCalculator = offsetCalculator;
    }

    public RestoreExecutionResult restore(
            String restoreType,
            Instant restoreFromTimestamp,
            RestoreJobExecutionContext context
    ) {
        EngineKafkaProperties.PipelineProperties pipeline =
                kafkaClientConfiguration.requirePipeline(restoreType);
        EngineKafkaProperties engineKafkaProperties =
                kafkaClientConfiguration.getEngineKafkaProperties();

        int emptyPolls = 0;
        int committedBatches = 0;
        long restoredRecords = 0L;

        try (
                KafkaConsumer<String, byte[]> consumer =
                        kafkaClientConfiguration.createConsumer(restoreType);
                KafkaProducer<String, byte[]> producer =
                        kafkaClientConfiguration.createProducer(restoreType)
        ) {
            consumer.subscribe(List.of(pipeline.getSourceTopic()));
            seekToStartingOffsets(
                    consumer,
                    pipeline.getSourceTopic(),
                    restoreFromTimestamp,
                    engineKafkaProperties.getPollTimeout()
            );

            Map<TopicPartition, Long> restoreEndOffsets = captureEndOffsets(consumer);
            Map<TopicPartition, Long> restoredPositions =
                    toPositions(currentOffsets(consumer, restoreEndOffsets.keySet()));

            log.info(
                    "Captured restore boundary for jobId={} restoreType={}: {}",
                    context.getJobId(),
                    restoreType,
                    restoreEndOffsets
            );

            if (hasReachedRestoreBoundary(restoredPositions, restoreEndOffsets)) {
                if (!context.tryMarkFinalizing()) {
                    handleFinalizingTransitionFailure(context);
                }
                log.info(
                        "Restore job {} reached the boundary before producing records; zero-record restore has no Kafka transaction",
                        context.getJobId()
                );
                return new RestoreExecutionResult(
                        restoreType,
                        pipeline.getSourceTopic(),
                        pipeline.getTargetTopic(),
                        pipeline.getGroupId(),
                        pipeline.getTransactionalId(),
                        pipeline.getMessageType(),
                        0,
                        0L,
                        0
                );
            }

            while (true) {
                context.throwIfCancellationRequested();
                ConsumerRecords<String, byte[]> polledRecords =
                        consumer.poll(engineKafkaProperties.getPollTimeout());
                if (polledRecords.isEmpty()) {
                    emptyPolls++;
                    if (emptyPolls >= engineKafkaProperties.getMaxEmptyPollsBeforeFinish()) {
                        throw new RestoreEngineException(
                                "Restore did not reach the captured boundary before empty poll limit. jobId="
                                        + context.getJobId()
                        );
                    }
                    continue;
                }

                emptyPolls = 0;
                List<ConsumerRecord<String, byte[]>> recordsToRestore =
                        filterRecordsWithinBoundary(polledRecords, restoreEndOffsets);
                if (recordsToRestore.isEmpty()) {
                    if (hasReachedRestoreBoundary(restoredPositions, restoreEndOffsets)) {
                        if (!context.tryMarkFinalizing()) {
                            handleFinalizingTransitionFailure(context);
                        }
                        log.info(
                                "Restore job {} exhausted the captured boundary without additional restorable records",
                                context.getJobId()
                        );
                        break;
                    }
                    log.info(
                            "Ignoring {} records at or beyond the restore boundary for jobId={}",
                            polledRecords.count(),
                            context.getJobId()
                    );
                    continue;
                }

                Map<TopicPartition, OffsetAndMetadata> offsets =
                        offsetCalculator.calculateOffsets(recordsToRestore);
                Map<TopicPartition, Long> nextRestoredPositions = new HashMap<>(restoredPositions);
                offsets.forEach(
                        (partition, offsetMetadata) ->
                                nextRestoredPositions.put(partition, offsetMetadata.offset()));
                boolean finalBatch =
                        hasReachedRestoreBoundary(nextRestoredPositions, restoreEndOffsets);

                restoreBatch(
                        producer,
                        consumer.groupMetadata(),
                        recordsToRestore,
                        offsets,
                        context,
                        pipeline.getTargetTopic(),
                        finalBatch
                );

                restoredPositions = nextRestoredPositions;
                committedBatches++;
                restoredRecords += recordsToRestore.size();
                log.info(
                        "Committed restore batch jobId={} batchNumber={} records={} finalBatch={}",
                        context.getJobId(),
                        committedBatches,
                        recordsToRestore.size(),
                        finalBatch
                );

                if (finalBatch) {
                    break;
                }
            }
        }

        log.info(
                "Restore finished for jobId={} restoreType={} sourceTopic={} targetTopic={} batchesCommitted={} recordsRestored={}",
                context.getJobId(),
                restoreType,
                pipeline.getSourceTopic(),
                pipeline.getTargetTopic(),
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
            ConsumerGroupMetadata groupMetadata,
            List<ConsumerRecord<String, byte[]>> records,
            Map<TopicPartition, OffsetAndMetadata> offsets,
            RestoreJobExecutionContext context,
            String targetTopic,
            boolean finalBatch
    ) {
        producer.beginTransaction();
        try {
            for (ConsumerRecord<String, byte[]> sourceRecord : records) {
                context.throwIfCancellationRequested();

                ProducerRecord<String, byte[]> targetRecord =
                        new ProducerRecord<>(
                                targetTopic,
                                sourceRecord.key(),
                                sourceRecord.value());

                producer.send(targetRecord).get();
            }

            producer.sendOffsetsToTransaction(offsets, groupMetadata);

            if (finalBatch) {
                if (!context.tryMarkFinalizing()) {
                    handleFinalizingTransitionFailure(context);
                }
                log.info("Restore job {} entered FINALIZING before final commit", context.getJobId());
            }

            // If commitTransaction() is ambiguous due to a network failure, this execution fails
            // and a future restart must continue with a new producer instance for the same stable transactional.id.
            producer.commitTransaction();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            abortTransactionSafely(producer, exception);
            throw new RestoreEngineException("Restore batch interrupted", exception);
        } catch (RestoreJobCancellationException exception) {
            abortTransactionSafely(producer, exception);
            throw exception;
        } catch (RuntimeException | ExecutionException exception) {
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

        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes =
                consumer.offsetsForTimes(timestampsToSearch);
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

    private Map<TopicPartition, OffsetAndMetadata> currentOffsets(
            KafkaConsumer<String, byte[]> consumer,
            Set<TopicPartition> partitions
    ) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition partition : partitions) {
            offsets.put(partition, new OffsetAndMetadata(consumer.position(partition)));
        }
        return offsets;
    }

    private Map<TopicPartition, Long> captureEndOffsets(KafkaConsumer<String, byte[]> consumer) {
        Set<TopicPartition> assignment = consumer.assignment();
        if (assignment.isEmpty()) {
            throw new RestoreEngineException(
                    "Consumer has no assigned partitions when capturing restore boundary"
            );
        }
        return Map.copyOf(consumer.endOffsets(assignment));
    }

    private List<ConsumerRecord<String, byte[]>> filterRecordsWithinBoundary(
            ConsumerRecords<String, byte[]> records,
            Map<TopicPartition, Long> restoreEndOffsets
    ) {
        List<ConsumerRecord<String, byte[]>> filteredRecords = new ArrayList<>();
        for (ConsumerRecord<String, byte[]> record : records) {
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            Long endOffset = restoreEndOffsets.get(topicPartition);
            if (endOffset != null && record.offset() < endOffset) {
                filteredRecords.add(record);
            }
        }
        return filteredRecords;
    }

    private Map<TopicPartition, Long> toPositions(
            Map<TopicPartition, OffsetAndMetadata> currentOffsets
    ) {
        Map<TopicPartition, Long> positions = new HashMap<>();
        currentOffsets.forEach(
                (partition, offsetMetadata) -> positions.put(partition, offsetMetadata.offset()));
        return positions;
    }

    private boolean hasReachedRestoreBoundary(
            Map<TopicPartition, Long> restoredPositions,
            Map<TopicPartition, Long> restoreEndOffsets
    ) {
        return restoreEndOffsets.entrySet().stream()
                .allMatch(
                        entry ->
                                restoredPositions.getOrDefault(entry.getKey(), Long.MIN_VALUE)
                                        >= entry.getValue());
    }

    private void handleFinalizingTransitionFailure(RestoreJobExecutionContext context) {
        RestoreJobStatus currentStatus = context.getStatus();
        if (currentStatus == RestoreJobStatus.CANCELLATION_REQUESTED) {
            throw new RestoreJobCancellationException(
                    "Cancellation won the finalizing race. jobId=" + context.getJobId()
            );
        }
        throw new RestoreJobStateException(
                "Restore job could not transition to FINALIZING from "
                        + currentStatus
                        + ". jobId="
                        + context.getJobId()
        );
    }

    private void abortTransactionSafely(
            KafkaProducer<String, byte[]> producer,
            Exception originalException
    ) {
        try {
            producer.abortTransaction();
            log.info("Aborted Kafka transaction after restore batch failure: {}", originalException.getMessage());
        } catch (RuntimeException abortException) {
            originalException.addSuppressed(abortException);
            log.warn(
                    "Kafka transaction abort also failed after restore batch failure",
                    abortException
            );
        }
    }
}
