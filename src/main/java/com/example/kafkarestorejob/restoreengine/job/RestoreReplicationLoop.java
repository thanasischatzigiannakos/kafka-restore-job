package com.example.kafkarestorejob.restoreengine.job;

import com.example.kafkarestorejob.restoreengine.config.EngineKafkaProperties;
import com.example.kafkarestorejob.restoreengine.config.KafkaClientConfiguration;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreEngineException;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreExecutionResult;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreTransformer;
import com.example.kafkarestorejob.restoreengine.validation.RestorePayloadHandler;
import com.example.kafkarestorejob.restoreengine.validation.RestorePayloadHandlerRegistry;
import com.example.kafkarestorejob.restoreengine.validation.RestoreRecordValidationContext;
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
    private final RestorePayloadHandlerRegistry payloadHandlerRegistry;
    private final RestoreTransformer transformer;

    public RestoreReplicationLoop(
            KafkaClientConfiguration kafkaClientConfiguration,
            RestorePayloadHandlerRegistry payloadHandlerRegistry,
            RestoreTransformer transformer
    ) {
        this.kafkaClientConfiguration = kafkaClientConfiguration;
        this.payloadHandlerRegistry = payloadHandlerRegistry;
        this.transformer = transformer;
    }

    public RestoreExecutionResult restore(
            String restoreType,
            EngineKafkaProperties.PipelineProperties pipeline,
            Instant restoreFromTimestamp,
            RestoreJobExecutionContext context,
            boolean resumeFromCommittedOffsets
    ) {
        return restoreInternal(
                restoreType,
                pipeline,
                restoreFromTimestamp,
                context,
                resumeFromCommittedOffsets
        );
    }

    private RestoreExecutionResult restoreInternal(
            String restoreType,
            EngineKafkaProperties.PipelineProperties pipeline,
            Instant restoreFromTimestamp,
            RestoreJobExecutionContext context,
            boolean resumeFromCommittedOffsets
    ) {
        EngineKafkaProperties engineKafkaProperties =
                kafkaClientConfiguration.getEngineKafkaProperties();
        RestorePayloadHandler payloadHandler =
                payloadHandlerRegistry.requireHandler(restoreType);

        int emptyPolls = 0;
        int committedTransactions = 0;
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
                    engineKafkaProperties.getPollTimeout(),
                    resumeFromCommittedOffsets
            );

            Map<TopicPartition, Long> restoreEndOffsets = captureEndOffsets(consumer);
            Map<TopicPartition, Long> restoredPositions =
                    toPositions(currentOffsets(consumer, restoreEndOffsets.keySet()));

            logRestoreBoundary(context, restoreType, restoreEndOffsets);

            if (isZeroRecordRestore(context, restoreEndOffsets, restoredPositions)) {
                return createResult(restoreType, pipeline, 0, 0L, 0);
            }

            RestoreLoopState loopState = new RestoreLoopState(restoredPositions);
            while (!loopState.isFinished()) {
                PollBatchOutcome pollBatchOutcome = pollAndRestoreBatch(
                        consumer,
                        producer,
                        consumer.groupMetadata(),
                        context,
                        restoreType,
                        pipeline,
                        restoreEndOffsets,
                        engineKafkaProperties,
                        loopState,
                        transformer,
                        payloadHandler
                );

                emptyPolls = pollBatchOutcome.emptyPolls();
                committedTransactions += pollBatchOutcome.committedTransactions();
                restoredRecords += pollBatchOutcome.restoredRecords();
            }
        }

        log.info(
                "Restore finished for jobId={} restoreType={} sourceTopic={} targetTopic={} committedTransactions={} recordsRestored={}",
                context.getJobId(),
                restoreType,
                pipeline.getSourceTopic(),
                pipeline.getTargetTopic(),
                committedTransactions,
                restoredRecords
        );

        return createResult(
                restoreType,
                pipeline,
                committedTransactions,
                restoredRecords,
                emptyPolls
        );
    }

    private PollBatchOutcome pollAndRestoreBatch(
            KafkaConsumer<String, byte[]> consumer,
            KafkaProducer<String, byte[]> producer,
            ConsumerGroupMetadata groupMetadata,
            RestoreJobExecutionContext context,
            String restoreType,
            EngineKafkaProperties.PipelineProperties pipeline,
            Map<TopicPartition, Long> restoreEndOffsets,
            EngineKafkaProperties engineKafkaProperties,
            RestoreLoopState loopState,
            RestoreTransformer transformer,
            RestorePayloadHandler payloadHandler
    ) {
        context.throwIfCancellationRequested();
        ConsumerRecords<String, byte[]> polledRecords =
                consumer.poll(engineKafkaProperties.getPollTimeout());
        if (polledRecords.isEmpty()) {
            return handleEmptyPoll(context, engineKafkaProperties, loopState);
        }

        loopState.resetEmptyPolls();
        return restorePolledRecords(
                producer,
                groupMetadata,
                context,
                restoreType,
                pipeline,
                loopState,
                polledRecords,
                restoreEndOffsets,
                transformer,
                payloadHandler
        );
    }

    private PollBatchOutcome handleEmptyPoll(
            RestoreJobExecutionContext context,
            EngineKafkaProperties engineKafkaProperties,
            RestoreLoopState loopState
    ) {
        int emptyPolls = loopState.incrementEmptyPolls();
        if (emptyPolls >= engineKafkaProperties.getMaxEmptyPollsBeforeFinish()) {
            throw new RestoreEngineException(
                    "Restore did not reach the captured boundary before empty poll limit. jobId="
                            + context.getJobId()
            );
        }
        return PollBatchOutcome.empty(emptyPolls);
    }

    private PollBatchOutcome restorePolledRecords(
            KafkaProducer<String, byte[]> producer,
            ConsumerGroupMetadata groupMetadata,
            RestoreJobExecutionContext context,
            String restoreType,
            EngineKafkaProperties.PipelineProperties pipeline,
            RestoreLoopState loopState,
            ConsumerRecords<String, byte[]> polledRecords,
            Map<TopicPartition, Long> restoreEndOffsets,
            RestoreTransformer transformer,
            RestorePayloadHandler payloadHandler
    ) {
        int ignoredRecords = 0;
        int restoredRecords = 0;
        int committedTransactions = 0;

        for (ConsumerRecord<String, byte[]> sourceRecord : polledRecords) {
            TopicPartition topicPartition = new TopicPartition(sourceRecord.topic(), sourceRecord.partition());
            Long endOffset = restoreEndOffsets.get(topicPartition);
            if (endOffset == null || sourceRecord.offset() >= endOffset) {
                ignoredRecords++;
                continue;
            }

            Map<TopicPartition, OffsetAndMetadata> offsets = offsetsForRecord(sourceRecord);
            Map<TopicPartition, Long> nextRestoredPositions = nextRestoredPositions(
                    loopState.getRestoredPositions(),
                    offsets
            );
            boolean finalRecord = hasReachedRestoreBoundary(nextRestoredPositions, restoreEndOffsets);

            restoreRecordTransaction(
                    producer,
                    groupMetadata,
                    sourceRecord,
                    offsets,
                    context,
                    restoreType,
                    pipeline,
                    finalRecord,
                    transformer,
                    payloadHandler
            );

            loopState.updateRestoredPositions(nextRestoredPositions);
            loopState.recordCommittedTransaction(finalRecord);
            restoredRecords++;
            committedTransactions++;
            logCommittedRecord(context, loopState.getCommittedTransactions(), sourceRecord, finalRecord);
        }

        if (restoredRecords == 0) {
            return handleEmptyRestorePoll(context, restoreEndOffsets, loopState, ignoredRecords);
        }

        return PollBatchOutcome.committed(
                loopState.getEmptyPolls(),
                committedTransactions,
                restoredRecords
        );
    }

    private Map<TopicPartition, Long> nextRestoredPositions(
            Map<TopicPartition, Long> restoredPositions,
            Map<TopicPartition, OffsetAndMetadata> offsets
    ) {
        Map<TopicPartition, Long> nextRestoredPositions = new HashMap<>(restoredPositions);
        offsets.forEach(
                (partition, offsetMetadata) ->
                        nextRestoredPositions.put(partition, offsetMetadata.offset()));
        return nextRestoredPositions;
    }

    private boolean isZeroRecordRestore(
            RestoreJobExecutionContext context,
            Map<TopicPartition, Long> restoreEndOffsets,
            Map<TopicPartition, Long> restoredPositions
    ) {
        if (!hasReachedRestoreBoundary(restoredPositions, restoreEndOffsets)) {
            return false;
        }

        markFinalizing(context);
        log.info(
                "Restore job {} reached the boundary before producing records; zero-record restore has no Kafka transaction",
                context.getJobId()
        );
        return true;
    }

    private void markFinalizing(RestoreJobExecutionContext context) {
        if (!context.tryMarkFinalizing()) {
            handleFinalizingTransitionFailure(context);
        }
    }

    private void logRestoreBoundary(
            RestoreJobExecutionContext context,
            String restoreType,
            Map<TopicPartition, Long> restoreEndOffsets
    ) {
        log.info(
                "Captured restore boundary for jobId={} restoreType={}: {}",
                context.getJobId(),
                restoreType,
                restoreEndOffsets
        );
    }

    private void logCommittedRecord(
            RestoreJobExecutionContext context,
            int committedTransactions,
            ConsumerRecord<String, byte[]> sourceRecord,
            boolean finalRecord
    ) {
        log.info(
                "Committed restore record jobId={} transactionNumber={} topic={} partition={} offset={} finalRecord={}",
                context.getJobId(),
                committedTransactions,
                sourceRecord.topic(),
                sourceRecord.partition(),
                sourceRecord.offset(),
                finalRecord
        );
    }

    private RestoreExecutionResult createResult(
            String restoreType,
            EngineKafkaProperties.PipelineProperties pipeline,
            int committedBatches,
            long restoredRecords,
            int emptyPolls
    ) {
        return new RestoreExecutionResult(
                restoreType,
                pipeline.getSourceTopic(),
                pipeline.getTargetTopic(),
                pipeline.getGroupId(),
                pipeline.getTransactionalId(),
                restoreType,
                committedBatches,
                restoredRecords,
                emptyPolls
        );
    }

    private RestoreEngineException restoreBatchFailure(Exception exception) {
        return new RestoreEngineException("Restore record failed", exception);
    }

    private void restoreRecordTransaction(
            KafkaProducer<String, byte[]> producer,
            ConsumerGroupMetadata groupMetadata,
            ConsumerRecord<String, byte[]> sourceRecord,
            Map<TopicPartition, OffsetAndMetadata> offsets,
            RestoreJobExecutionContext context,
            String restoreType,
            EngineKafkaProperties.PipelineProperties pipeline,
            boolean finalRecord,
            RestoreTransformer transformer,
            RestorePayloadHandler payloadHandler
    ) {
        producer.beginTransaction();
        try {
            context.throwIfCancellationRequested();
            payloadHandler.validate(
                    buildValidationContext(context, restoreType, sourceRecord),
                    sourceRecord.value()
            );
            ProducerRecord<String, byte[]> targetRecord =
                    transformer.transform(restoreType, pipeline.getTargetTopic(), sourceRecord);

            producer.send(targetRecord).get();

            producer.sendOffsetsToTransaction(offsets, groupMetadata);

            if (finalRecord) {
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
            throw new RestoreEngineException("Restore record interrupted", exception);
        } catch (RestoreJobCancellationException exception) {
            abortTransactionSafely(producer, exception);
            throw exception;
        } catch (RuntimeException | ExecutionException exception) {
            abortTransactionSafely(producer, exception);
            throw restoreBatchFailure(exception);
        }
    }

    private PollBatchOutcome handleEmptyRestorePoll(
            RestoreJobExecutionContext context,
            Map<TopicPartition, Long> restoreEndOffsets,
            RestoreLoopState loopState,
            int ignoredRecordCount
    ) {
        if (hasReachedRestoreBoundary(loopState.getRestoredPositions(), restoreEndOffsets)) {
            markFinalizing(context);
            log.info(
                    "Restore job {} exhausted the captured boundary without additional restorable records",
                    context.getJobId()
            );
            loopState.finish();
            return PollBatchOutcome.noCommit(loopState.getEmptyPolls());
        }

        if (ignoredRecordCount > 0) {
            log.info(
                    "Ignoring {} records at or beyond the restore boundary for jobId={}",
                    ignoredRecordCount,
                    context.getJobId()
            );
        }
        return PollBatchOutcome.noCommit(loopState.getEmptyPolls());
    }

    private RestoreRecordValidationContext buildValidationContext(
            RestoreJobExecutionContext context,
            String restoreType,
            ConsumerRecord<String, byte[]> sourceRecord
    ) {
        return new RestoreRecordValidationContext(
                context.getJobId(),
                restoreType,
                sourceRecord.topic(),
                sourceRecord.partition(),
                sourceRecord.offset()
        );
    }

    private void seekToStartingOffsets(
            KafkaConsumer<String, byte[]> consumer,
            String sourceTopic,
            Instant restoreFromTimestamp,
            Duration assignmentPollTimeout,
            boolean resumeFromCommittedOffsets
    ) {
        Set<TopicPartition> assignment = awaitAssignment(consumer, assignmentPollTimeout);
        if (resumeFromCommittedOffsets) {
            Map<TopicPartition, OffsetAndMetadata> committedOffsets = consumer.committed(assignment);
            List<TopicPartition> partitionsWithoutCommittedOffsets = new ArrayList<>();
            for (TopicPartition topicPartition : assignment) {
                OffsetAndMetadata committedOffset = committedOffsets.get(topicPartition);
                if (committedOffset == null) {
                    partitionsWithoutCommittedOffsets.add(topicPartition);
                    continue;
                }

                consumer.seek(topicPartition, committedOffset.offset());
                log.info(
                        "Restore resumes at committed offset={} for sourceTopic={} partition={}",
                        committedOffset.offset(),
                        sourceTopic,
                        topicPartition.partition()
                );
            }

            if (partitionsWithoutCommittedOffsets.isEmpty()) {
                return;
            }

            seekPartitionsWithoutCommittedOffsets(
                    consumer,
                    sourceTopic,
                    restoreFromTimestamp,
                    partitionsWithoutCommittedOffsets
            );
            return;
        }

        seekPartitionsWithoutCommittedOffsets(
                consumer,
                sourceTopic,
                restoreFromTimestamp,
                assignment
        );
    }

    private void seekPartitionsWithoutCommittedOffsets(
            KafkaConsumer<String, byte[]> consumer,
            String sourceTopic,
            Instant restoreFromTimestamp,
            Iterable<TopicPartition> partitions
    ) {
        if (restoreFromTimestamp == null) {
            List<TopicPartition> partitionsToSeek = toPartitionList(partitions);
            consumer.seekToBeginning(partitionsToSeek);
            log.info("Restore starts at beginning for sourceTopic={}", sourceTopic);
            return;
        }

        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        for (TopicPartition topicPartition : partitions) {
            timestampsToSearch.put(topicPartition, restoreFromTimestamp.toEpochMilli());
        }

        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes =
                consumer.offsetsForTimes(timestampsToSearch);
        for (TopicPartition topicPartition : timestampsToSearch.keySet()) {
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

    private List<TopicPartition> toPartitionList(Iterable<TopicPartition> partitions) {
        List<TopicPartition> partitionList = new ArrayList<>();
        for (TopicPartition partition : partitions) {
            partitionList.add(partition);
        }
        return partitionList;
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

    private Map<TopicPartition, OffsetAndMetadata> offsetsForRecord(
            ConsumerRecord<String, byte[]> sourceRecord
    ) {
        return Map.of(
                new TopicPartition(sourceRecord.topic(), sourceRecord.partition()),
                new OffsetAndMetadata(sourceRecord.offset() + 1)
        );
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

    private static final class RestoreLoopState {

        private Map<TopicPartition, Long> restoredPositions;
        private int emptyPolls;
        private int committedTransactions;
        private boolean finished;

        private RestoreLoopState(Map<TopicPartition, Long> restoredPositions) {
            this.restoredPositions = restoredPositions;
        }

        private Map<TopicPartition, Long> getRestoredPositions() {
            return restoredPositions;
        }

        private int getEmptyPolls() {
            return emptyPolls;
        }

        private int getCommittedTransactions() {
            return committedTransactions;
        }

        private boolean isFinished() {
            return finished;
        }

        private int incrementEmptyPolls() {
            emptyPolls++;
            return emptyPolls;
        }

        private void resetEmptyPolls() {
            emptyPolls = 0;
        }

        private void updateRestoredPositions(Map<TopicPartition, Long> nextRestoredPositions) {
            restoredPositions = nextRestoredPositions;
        }

        private void recordCommittedTransaction(boolean finalRecord) {
            committedTransactions++;
            finished = finalRecord;
        }

        private void finish() {
            finished = true;
        }
    }

    private record PollBatchOutcome(
            int emptyPolls,
            int committedTransactions,
            long restoredRecords
    ) {

        private static PollBatchOutcome empty(int emptyPolls) {
            return new PollBatchOutcome(emptyPolls, 0, 0L);
        }

        private static PollBatchOutcome noCommit(int emptyPolls) {
            return new PollBatchOutcome(emptyPolls, 0, 0L);
        }

        private static PollBatchOutcome committed(int emptyPolls, long restoredRecords) {
            return new PollBatchOutcome(emptyPolls, 1, restoredRecords);
        }
    }
}
