package com.example.kafkarestorejob.restoreengine.job;

import com.example.kafkarestorejob.restoreengine.config.EngineKafkaProperties;
import com.example.kafkarestorejob.restoreengine.config.KafkaClientConfiguration;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreEngineException;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreExecutionResult;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreTransformer;
import com.example.kafkarestorejob.restoreengine.validation.MessageHandler;
import com.example.kafkarestorejob.restoreengine.validation.MessageHandlerRegistry;
import com.example.kafkarestorejob.restoreengine.validation.MessageTypeMismatchException;
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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Orchestrates the restore flow for one configured restore type.
 *
 * <p>The loop consumes source records, validates each record through the resolved message handler,
 * writes the original bytes to the target topic, and advances the source offset only after the
 * target transaction commits.
 */
@Component
public class RestoreReplicationLoop {

    private static final Logger log = LoggerFactory.getLogger(RestoreReplicationLoop.class);

    private final KafkaClientConfiguration kafkaClientConfiguration;
    private final MessageHandlerRegistry messageHandlerRegistry;
    private final RestoreTransformer transformer;

    public RestoreReplicationLoop(
            KafkaClientConfiguration kafkaClientConfiguration,
            MessageHandlerRegistry messageHandlerRegistry,
            RestoreTransformer transformer
    ) {
        this.kafkaClientConfiguration = kafkaClientConfiguration;
        this.messageHandlerRegistry = messageHandlerRegistry;
        this.transformer = transformer;
    }

    /**
     * Executes the restore loop for one configured restore type.
     *
     * @param restoreType the logical restore type being executed
     * @param pipeline the configured source and target topics plus client identities
     * @param restoreFromTimestamp the optional timestamp boundary used when starting from time-based
     *                             offsets
     * @param context the mutable job execution context
     * @param resumeFromCommittedOffsets whether to resume from committed source offsets
     * @return the restore execution summary
     */
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

    /**
     * Performs the full restore lifecycle including assignment, seek, boundary capture, polling,
     * validation, target writes, and source offset commits.
     *
     * @param restoreType the logical restore type being executed
     * @param pipeline the configured pipeline
     * @param restoreFromTimestamp the optional time-based starting point
     * @param context the mutable job execution context
     * @param resumeFromCommittedOffsets whether to resume from committed source offsets
     * @return the restore execution summary
     */
    private RestoreExecutionResult restoreInternal(
            String restoreType,
            EngineKafkaProperties.PipelineProperties pipeline,
            Instant restoreFromTimestamp,
            RestoreJobExecutionContext context,
            boolean resumeFromCommittedOffsets
    ) {
        EngineKafkaProperties engineKafkaProperties =
                kafkaClientConfiguration.getEngineKafkaProperties();
        MessageHandler messageHandler =
                messageHandlerRegistry.requireHandler(restoreType);

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
                PollOutcome pollOutcome = pollAndRestoreRecords(
                        consumer,
                        producer,
                        context,
                        restoreType,
                        pipeline,
                        restoreEndOffsets,
                        engineKafkaProperties,
                        loopState,
                        transformer,
                        messageHandler
                );

                emptyPolls = pollOutcome.emptyPolls();
                committedTransactions += pollOutcome.committedTransactions();
                restoredRecords += pollOutcome.restoredRecords();
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

    /**
     * Polls Kafka once and restores any records that fall within the captured restore boundary.
     *
     * @param consumer the source consumer
     * @param producer the target producer
     * @param context the mutable job execution context
     * @param restoreType the logical restore type being executed
     * @param pipeline the configured pipeline
     * @param restoreEndOffsets the fixed exclusive boundary captured at restore start
     * @param engineKafkaProperties the bound Kafka runtime settings
     * @param loopState the mutable loop state
     * @param transformer the target-record transformer
     * @param messageHandler the resolved per-type validator
     * @return the outcome of the poll iteration
     */
    private PollOutcome pollAndRestoreRecords(
            KafkaConsumer<String, byte[]> consumer,
            KafkaProducer<String, byte[]> producer,
            RestoreJobExecutionContext context,
            String restoreType,
            EngineKafkaProperties.PipelineProperties pipeline,
            Map<TopicPartition, Long> restoreEndOffsets,
            EngineKafkaProperties engineKafkaProperties,
            RestoreLoopState loopState,
            RestoreTransformer transformer,
            MessageHandler messageHandler
    ) {
        context.throwIfCancellationRequested();
        ConsumerRecords<String, byte[]> polledRecords =
                consumer.poll(engineKafkaProperties.getPollTimeout());
        if (polledRecords.isEmpty()) {
            return handleEmptyPoll(context, engineKafkaProperties, loopState);
        }

        loopState.resetEmptyPolls();
        return restorePolledRecords(
                consumer,
                producer,
                context,
                restoreType,
                pipeline,
                loopState,
                polledRecords,
                restoreEndOffsets,
                transformer,
                messageHandler
        );
    }

    /**
     * Handles an empty poll by incrementing the empty-poll counter and failing when the configured
     * limit is reached before the boundary is exhausted.
     *
     * @param context the mutable job execution context
     * @param engineKafkaProperties the bound Kafka runtime settings
     * @param loopState the mutable loop state
     * @return the empty poll outcome
     */
    private PollOutcome handleEmptyPoll(
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
        return PollOutcome.empty(emptyPolls);
    }

    /**
     * Restores each restorable source record from one Kafka poll result.
     *
     * @param consumer the source consumer
     * @param producer the target producer
     * @param context the mutable job execution context
     * @param restoreType the logical restore type being executed
     * @param pipeline the configured pipeline
     * @param loopState the mutable loop state
     * @param polledRecords the source records returned by Kafka
     * @param restoreEndOffsets the fixed exclusive boundary captured at restore start
     * @param transformer the target-record transformer
     * @param messageHandler the resolved per-type validator
     * @return the outcome of restoring the poll result
     */
    private PollOutcome restorePolledRecords(
            KafkaConsumer<String, byte[]> consumer,
            KafkaProducer<String, byte[]> producer,
            RestoreJobExecutionContext context,
            String restoreType,
            EngineKafkaProperties.PipelineProperties pipeline,
            RestoreLoopState loopState,
            ConsumerRecords<String, byte[]> polledRecords,
            Map<TopicPartition, Long> restoreEndOffsets,
            RestoreTransformer transformer,
            MessageHandler messageHandler
    ) {
        int ignoredRecords = 0;
        int restoredRecords = 0;
        int skippedTypeMismatches = 0;
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

            RecordTransactionOutcome transactionOutcome = restoreRecordTransaction(
                    consumer,
                    producer,
                    sourceRecord,
                    offsets,
                    context,
                    restoreType,
                    pipeline,
                    finalRecord,
                    transformer,
                    messageHandler
            );

            loopState.updateRestoredPositions(nextRestoredPositions);
            loopState.recordCommittedTransaction(finalRecord);
            if (transactionOutcome == RecordTransactionOutcome.RESTORED) {
                restoredRecords++;
                committedTransactions++;
            } else {
                skippedTypeMismatches++;
            }
            logCommittedRecord(
                    context,
                    loopState.getCommittedTransactions(),
                    sourceRecord,
                    finalRecord,
                    transactionOutcome
            );
        }

        if (restoredRecords == 0) {
            return handleEmptyRestorePoll(
                    context,
                    restoreEndOffsets,
                    loopState,
                    ignoredRecords,
                    skippedTypeMismatches,
                    committedTransactions
            );
        }

        return PollOutcome.committed(
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
            boolean finalRecord,
            RecordTransactionOutcome transactionOutcome
    ) {
        log.info(
                "Committed restore transaction jobId={} transactionNumber={} topic={} partition={} offset={} finalRecord={} outcome={}",
                context.getJobId(),
                committedTransactions,
                sourceRecord.topic(),
                sourceRecord.partition(),
                sourceRecord.offset(),
                finalRecord,
                transactionOutcome
        );
    }

    private RestoreExecutionResult createResult(
            String restoreType,
            EngineKafkaProperties.PipelineProperties pipeline,
            int committedTransactions,
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
                committedTransactions,
                restoredRecords,
                emptyPolls
        );
    }

    private RestoreEngineException restoreRecordFailure(Exception exception) {
        return new RestoreEngineException("Restore record failed", exception);
    }

    /**
     * Restores one source record in its own target transaction and commits the source offset only
     * after the target commit succeeds.
     *
     * @param consumer the source consumer
     * @param producer the target producer
     * @param sourceRecord the source record being restored
     * @param offsets the next source offsets to commit for this record
     * @param context the mutable job execution context
     * @param restoreType the logical restore type being executed
     * @param pipeline the configured pipeline
     * @param finalRecord whether committing this record reaches the captured restore boundary
     * @param transformer the target-record transformer
     * @param messageHandler the resolved per-type validator
     * @return the outcome of the per-record transaction
     */
    private RecordTransactionOutcome restoreRecordTransaction(
            KafkaConsumer<String, byte[]> consumer,
            KafkaProducer<String, byte[]> producer,
            ConsumerRecord<String, byte[]> sourceRecord,
            Map<TopicPartition, OffsetAndMetadata> offsets,
            RestoreJobExecutionContext context,
            String restoreType,
            EngineKafkaProperties.PipelineProperties pipeline,
            boolean finalRecord,
            RestoreTransformer transformer,
            MessageHandler messageHandler
    ) {
        boolean transactionStarted = false;
        try {
            context.throwIfCancellationRequested();
            messageHandler.validate(
                    sourceRecord,
                    buildValidationContext(context, restoreType, sourceRecord)
            );
            producer.beginTransaction();
            transactionStarted = true;
            ProducerRecord<String, byte[]> targetRecord =
                    transformer.transform(restoreType, pipeline.getTargetTopic(), sourceRecord);

            producer.send(targetRecord).get();
            commitProducerTransaction(producer, context, finalRecord);
            commitSourceOffsets(consumer, offsets, context, sourceRecord, restoreType);
            return RecordTransactionOutcome.RESTORED;
        } catch (MessageTypeMismatchException exception) {
            log.warn(
                    "Skipping record due to payload type mismatch jobId={} restoreType={} topic={} partition={} offset={} reason={}",
                    context.getJobId(),
                    restoreType,
                    sourceRecord.topic(),
                    sourceRecord.partition(),
                    sourceRecord.offset(),
                    exception.getMessage()
            );
            if (finalRecord) {
                markFinalizing(context);
            }
            commitSourceOffsets(consumer, offsets, context, sourceRecord, restoreType);
            return RecordTransactionOutcome.SKIPPED_TYPE_MISMATCH;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            abortTransactionIfStarted(producer, exception, transactionStarted);
            throw new RestoreEngineException("Restore record interrupted", exception);
        } catch (RestoreJobCancellationException exception) {
            abortTransactionIfStarted(producer, exception, transactionStarted);
            throw exception;
        } catch (RuntimeException | ExecutionException exception) {
            abortTransactionIfStarted(producer, exception, transactionStarted);
            throw restoreRecordFailure(exception);
        }
    }

    /**
     * Commits the target-side transaction and marks the job as finalizing when the final restore
     * boundary record is about to commit.
     *
     * @param producer the target producer
     * @param context the mutable job execution context
     * @param finalRecord whether this commit reaches the restore boundary
     */
    private void commitProducerTransaction(
            KafkaProducer<String, byte[]> producer,
            RestoreJobExecutionContext context,
            boolean finalRecord
    ) {
        if (finalRecord) {
            if (!context.tryMarkFinalizing()) {
                handleFinalizingTransitionFailure(context);
            }
            log.info("Restore job {} entered FINALIZING before final commit", context.getJobId());
        }

        // If commitTransaction() is ambiguous due to a network failure, this execution fails
        // and a future restart must continue with a new producer instance for the same stable transactional.id.
        producer.commitTransaction();
    }

    /**
     * Commits the source consumer offsets only after the corresponding target transaction has
     * committed successfully.
     *
     * @param consumer the source consumer
     * @param offsets the next source offsets to commit
     * @param context the mutable job execution context
     * @param sourceRecord the source record whose offset is being committed
     * @param restoreType the logical restore type being executed
     */
    private void commitSourceOffsets(
            KafkaConsumer<String, byte[]> consumer,
            Map<TopicPartition, OffsetAndMetadata> offsets,
            RestoreJobExecutionContext context,
            ConsumerRecord<String, byte[]> sourceRecord,
            String restoreType
    ) {
        try {
            consumer.commitSync(offsets);
        } catch (RuntimeException exception) {
            throw new RestoreEngineException(
                    "Target record was committed but source offset commit failed. jobId="
                            + context.getJobId()
                            + " restoreType="
                            + restoreType
                            + " topic="
                            + sourceRecord.topic()
                            + " partition="
                            + sourceRecord.partition()
                            + " offset="
                            + sourceRecord.offset(),
                    exception
            );
        }
    }

    private PollOutcome handleEmptyRestorePoll(
            RestoreJobExecutionContext context,
            Map<TopicPartition, Long> restoreEndOffsets,
            RestoreLoopState loopState,
            int ignoredRecordCount,
            int skippedTypeMismatchCount,
            int committedTransactions
    ) {
        if (hasReachedRestoreBoundary(loopState.getRestoredPositions(), restoreEndOffsets)) {
            markFinalizing(context);
            log.info(
                    "Restore job {} exhausted the captured boundary without additional restorable records",
                    context.getJobId()
            );
            loopState.finish();
            return PollOutcome.noCommit(loopState.getEmptyPolls());
        }

        if (ignoredRecordCount > 0) {
            log.info(
                    "Ignoring {} records at or beyond the restore boundary for jobId={}",
                    ignoredRecordCount,
                    context.getJobId()
            );
        }
        if (skippedTypeMismatchCount > 0) {
            log.info(
                    "Skipped {} records due to payload type mismatch for jobId={}",
                    skippedTypeMismatchCount,
                    context.getJobId()
            );
        }
        if (committedTransactions > 0) {
            return PollOutcome.committed(loopState.getEmptyPolls(), committedTransactions, 0L);
        }
        return PollOutcome.noCommit(loopState.getEmptyPolls());
    }

    /**
     * Builds the record-level validation context used for diagnostics in the message handlers.
     *
     * @param context the mutable job execution context
     * @param restoreType the logical restore type being executed
     * @param sourceRecord the source record under validation
     * @return the validation context
     */
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

    /**
     * Positions the consumer either at committed offsets or at the configured start point for any
     * partition that has no committed offset yet.
     *
     * @param consumer the source consumer
     * @param sourceTopic the source topic name
     * @param restoreFromTimestamp the optional time-based starting point
     * @param assignmentPollTimeout the timeout used to wait for partition assignment
     * @param resumeFromCommittedOffsets whether committed offsets should be used when present
     */
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

    /**
     * Seeks the provided partitions either to the beginning or to the offsets resolved from the
     * requested timestamp.
     *
     * @param consumer the source consumer
     * @param sourceTopic the source topic name
     * @param restoreFromTimestamp the optional time-based starting point
     * @param partitions the partitions that still need an initial position
     */
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

    /**
     * Waits until Kafka assigns at least one partition to the consumer.
     *
     * @param consumer the source consumer
     * @param assignmentPollTimeout the timeout used while waiting for assignment
     * @return the assigned partitions
     */
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

    /**
     * Captures the exclusive end offsets that define the immutable restore boundary for this run.
     *
     * @param consumer the source consumer
     * @return the captured end offsets per assigned partition
     */
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

    /**
     * Aborts the current producer transaction and preserves any abort failure as a suppressed
     * exception on the original failure.
     *
     * @param producer the target producer
     * @param originalException the failure that triggered the abort
     */
    private void abortTransactionSafely(
            KafkaProducer<String, byte[]> producer,
            Exception originalException
    ) {
        try {
            producer.abortTransaction();
            log.info("Aborted Kafka transaction after restore record failure: {}", originalException.getMessage());
        } catch (RuntimeException abortException) {
            originalException.addSuppressed(abortException);
            log.warn(
                    "Kafka transaction abort also failed after restore record failure",
                    abortException
            );
        }
    }

    private void abortTransactionIfStarted(
            KafkaProducer<String, byte[]> producer,
            Exception originalException,
            boolean transactionStarted
    ) {
        if (!transactionStarted) {
            return;
        }
        abortTransactionSafely(producer, originalException);
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

    private record PollOutcome(
            int emptyPolls,
            int committedTransactions,
            long restoredRecords
    ) {

        private static PollOutcome empty(int emptyPolls) {
            return new PollOutcome(emptyPolls, 0, 0L);
        }

        private static PollOutcome noCommit(int emptyPolls) {
            return new PollOutcome(emptyPolls, 0, 0L);
        }

        private static PollOutcome committed(
                int emptyPolls,
                int committedTransactions,
                long restoredRecords
        ) {
            return new PollOutcome(emptyPolls, committedTransactions, restoredRecords);
        }
    }

    private enum RecordTransactionOutcome {
        RESTORED,
        SKIPPED_TYPE_MISMATCH
    }
}
