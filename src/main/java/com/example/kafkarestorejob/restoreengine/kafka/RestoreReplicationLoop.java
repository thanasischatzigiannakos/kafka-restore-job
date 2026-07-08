package com.example.kafkarestorejob.restoreengine.kafka;

import com.example.kafkarestorejob.restoreengine.config.EngineKafkaProperties;
import com.example.kafkarestorejob.restoreengine.config.KafkaClientConfiguration;
import com.example.kafkarestorejob.restoreengine.job.RestoreJobCancellationException;
import com.example.kafkarestorejob.restoreengine.serialization.RestoreMessageUnpackerResolver;
import com.example.kafkarestorejob.restoreengine.verification.BinaryVerificationService;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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

    public RestoreExecutionResult restore(String restoreType, BooleanSupplier cancellationRequested) {
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
                "Restore finished for restoreType={} sourceTopic={} targetTopic={} batchesCommitted={} recordsRestored={}",
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
        } catch (ExecutionException | RuntimeException exception) {
            abortTransactionSafely(producer, exception);
            throw new RestoreEngineException("Restore batch failed", exception);
        }
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
