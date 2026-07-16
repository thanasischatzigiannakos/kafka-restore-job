package com.example.kafkarestorejob.restoreengine.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.kafkarestorejob.restoreengine.config.EngineKafkaProperties;
import com.example.kafkarestorejob.restoreengine.config.KafkaClientConfiguration;
import com.example.kafkarestorejob.restoreengine.kafka.KafkaOffsetCalculator;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreEngineException;
import com.example.kafkarestorejob.restoreengine.kafka.RestoreExecutionResult;
import com.example.kafkarestorejob.restoreengine.processing.RestoreMessageProcessor;
import com.example.kafkarestorejob.restoreengine.processing.RestoreMessageProcessorResolver;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RestoreReplicationLoopTest {

    private static final String RESTORE_TYPE = "application";
    private static final String SOURCE_TOPIC = "source-topic";
    private static final String TARGET_TOPIC = "target-topic";
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition(SOURCE_TOPIC, 0);

    @Mock
    private KafkaClientConfiguration kafkaClientConfiguration;

    @Mock
    private KafkaConsumer<String, byte[]> consumer;

    @Mock
    private KafkaProducer<String, byte[]> producer;

    @Mock
    private RestoreMessageProcessorResolver processorResolver;

    @Mock
    private RestoreMessageProcessor processor;

    private RestoreReplicationLoop restoreReplicationLoop;
    private EngineKafkaProperties engineKafkaProperties;
    private EngineKafkaProperties.PipelineProperties pipelineProperties;
    private ConsumerGroupMetadata groupMetadata;

    @BeforeEach
    void setUp() {
        restoreReplicationLoop = new RestoreReplicationLoop(
                kafkaClientConfiguration,
                new KafkaOffsetCalculator(),
                processorResolver
        );

        engineKafkaProperties = new EngineKafkaProperties();
        engineKafkaProperties.setPollTimeout(Duration.ofSeconds(1));
        engineKafkaProperties.setMaxEmptyPollsBeforeFinish(2);

        pipelineProperties = new EngineKafkaProperties.PipelineProperties();
        pipelineProperties.setSourceTopic(SOURCE_TOPIC);
        pipelineProperties.setTargetTopic(TARGET_TOPIC);
        pipelineProperties.setGroupId("restore-group");
        pipelineProperties.setTransactionalId("restore-tx");
        pipelineProperties.setMessageType("application");
        pipelineProperties.setBatchSize(100);

        groupMetadata = new ConsumerGroupMetadata("restore-group");

        when(kafkaClientConfiguration.requirePipeline(RESTORE_TYPE)).thenReturn(pipelineProperties);
        when(kafkaClientConfiguration.getEngineKafkaProperties()).thenReturn(engineKafkaProperties);
        when(kafkaClientConfiguration.createConsumer(RESTORE_TYPE)).thenReturn(consumer);
        when(kafkaClientConfiguration.createProducer(RESTORE_TYPE)).thenReturn(producer);
        when(consumer.assignment()).thenReturn(java.util.Set.of(TOPIC_PARTITION));
        when(consumer.groupMetadata()).thenReturn(groupMetadata);
        when(processorResolver.resolve("application")).thenReturn(processor);
        when(processor.transform(eq(TARGET_TOPIC), any())).thenAnswer(invocation -> {
            ConsumerRecord<String, byte[]> sourceRecord = invocation.getArgument(1);
            return new ProducerRecord<>(
                    TARGET_TOPIC,
                    sourceRecord.partition(),
                    sourceRecord.key(),
                    sourceRecord.value()
            );
        });
        when(producer.send(any(ProducerRecord.class))).thenAnswer(
                invocation -> CompletableFuture.completedFuture(null));
    }

    @Test
    void commitsBatchInTransactionalOrder() {
        RestoreJobExecutionContext context = runningContext();
        when(consumer.poll(any(Duration.class))).thenReturn(
                ConsumerRecords.empty(),
                records(record(0L), record(1L))
        );
        when(consumer.position(TOPIC_PARTITION)).thenReturn(0L);
        when(consumer.endOffsets(java.util.Set.of(TOPIC_PARTITION))).thenReturn(Map.of(TOPIC_PARTITION, 2L));

        restoreReplicationLoop.restore(RESTORE_TYPE, null, context);

        verify(processor, times(2)).inspect(eq(RESTORE_TYPE), any());
        verify(producer).beginTransaction();
        verify(producer, times(2)).send(any(ProducerRecord.class));
        verify(producer).sendOffsetsToTransaction(anyMap(), eq(groupMetadata));
        verify(producer).commitTransaction();
    }

    @Test
    void sendsRecordsSequentially() {
        RestoreJobExecutionContext context = runningContext();
        when(consumer.poll(any(Duration.class))).thenReturn(
                ConsumerRecords.empty(),
                records(record(0L), record(1L), record(2L))
        );
        when(consumer.position(TOPIC_PARTITION)).thenReturn(0L);
        when(consumer.endOffsets(java.util.Set.of(TOPIC_PARTITION))).thenReturn(Map.of(TOPIC_PARTITION, 3L));

        restoreReplicationLoop.restore(RESTORE_TYPE, null, context);

        ArgumentCaptor<ProducerRecord<String, byte[]>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer, times(3)).send(captor.capture());
        assertEquals(List.of("key-0", "key-1", "key-2"),
                captor.getAllValues().stream().map(ProducerRecord::key).toList());
    }

    @Test
    void abortsTransactionWhenSendFails() throws Exception {
        RestoreJobExecutionContext context = runningContext();
        java.util.concurrent.Future<?> failingFuture = mock(java.util.concurrent.Future.class);
        when(failingFuture.get()).thenThrow(new ExecutionException(new RuntimeException("send failed")));
        when(producer.send(any(ProducerRecord.class))).thenReturn((java.util.concurrent.Future) failingFuture);
        when(consumer.poll(any(Duration.class))).thenReturn(
                ConsumerRecords.empty(),
                records(record(0L), record(1L))
        );
        when(consumer.position(TOPIC_PARTITION)).thenReturn(0L);
        when(consumer.endOffsets(java.util.Set.of(TOPIC_PARTITION))).thenReturn(Map.of(TOPIC_PARTITION, 2L));

        assertThrows(RestoreEngineException.class,
                () -> restoreReplicationLoop.restore(RESTORE_TYPE, null, context));

        verify(producer).abortTransaction();
        verify(producer, never()).sendOffsetsToTransaction(anyMap(), any());
    }

    @Test
    void abortsTransactionWhenOffsetSubmissionFails() {
        RestoreJobExecutionContext context = runningContext();
        doThrow(new RuntimeException("offset failure")).when(producer)
                .sendOffsetsToTransaction(anyMap(), eq(groupMetadata));
        when(consumer.poll(any(Duration.class))).thenReturn(
                ConsumerRecords.empty(),
                records(record(0L), record(1L))
        );
        when(consumer.position(TOPIC_PARTITION)).thenReturn(0L);
        when(consumer.endOffsets(java.util.Set.of(TOPIC_PARTITION))).thenReturn(Map.of(TOPIC_PARTITION, 2L));

        assertThrows(RestoreEngineException.class,
                () -> restoreReplicationLoop.restore(RESTORE_TYPE, null, context));

        verify(producer).abortTransaction();
    }

    @Test
    void abortsTransactionWhenCommitFails() {
        RestoreJobExecutionContext context = runningContext();
        doThrow(new RuntimeException("commit failure")).when(producer).commitTransaction();
        when(consumer.poll(any(Duration.class))).thenReturn(
                ConsumerRecords.empty(),
                records(record(0L), record(1L))
        );
        when(consumer.position(TOPIC_PARTITION)).thenReturn(0L);
        when(consumer.endOffsets(java.util.Set.of(TOPIC_PARTITION))).thenReturn(Map.of(TOPIC_PARTITION, 2L));

        assertThrows(RestoreEngineException.class,
                () -> restoreReplicationLoop.restore(RESTORE_TYPE, null, context));

        verify(producer).abortTransaction();
    }

    @Test
    void doesNotCommitPartialBatchOffsets() throws Exception {
        RestoreJobExecutionContext context = runningContext();
        java.util.concurrent.Future<?> firstFuture = mock(java.util.concurrent.Future.class);
        when(firstFuture.get()).thenReturn(null);
        java.util.concurrent.Future<?> secondFuture = mock(java.util.concurrent.Future.class);
        when(secondFuture.get()).thenThrow(new ExecutionException(new RuntimeException("send failed")));
        when(producer.send(any(ProducerRecord.class))).thenReturn(
                (java.util.concurrent.Future) firstFuture,
                (java.util.concurrent.Future) secondFuture
        );
        when(consumer.poll(any(Duration.class))).thenReturn(
                ConsumerRecords.empty(),
                records(record(0L), record(1L))
        );
        when(consumer.position(TOPIC_PARTITION)).thenReturn(0L);
        when(consumer.endOffsets(java.util.Set.of(TOPIC_PARTITION))).thenReturn(Map.of(TOPIC_PARTITION, 2L));

        assertThrows(RestoreEngineException.class,
                () -> restoreReplicationLoop.restore(RESTORE_TYPE, null, context));

        verify(producer, never()).sendOffsetsToTransaction(anyMap(), any());
    }

    @Test
    void usesExclusiveEndOffsetsAndIgnoresRecordsAtBoundaryOrAbove() {
        RestoreJobExecutionContext context = runningContext();
        when(consumer.poll(any(Duration.class))).thenReturn(
                ConsumerRecords.empty(),
                records(record(0L), record(1L), record(2L))
        );
        when(consumer.position(TOPIC_PARTITION)).thenReturn(0L);
        when(consumer.endOffsets(java.util.Set.of(TOPIC_PARTITION))).thenReturn(Map.of(TOPIC_PARTITION, 2L));

        RestoreExecutionResult result = restoreReplicationLoop.restore(RESTORE_TYPE, null, context);

        verify(producer, times(2)).send(any(ProducerRecord.class));
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsetsCaptor =
                ArgumentCaptor.forClass(Map.class);
        verify(producer).sendOffsetsToTransaction(offsetsCaptor.capture(), eq(groupMetadata));
        assertEquals(2L, offsetsCaptor.getValue().get(TOPIC_PARTITION).offset());
        assertEquals(2L, result.recordsRestored());
        assertEquals(RestoreJobStatus.FINALIZING, context.getStatus());
    }

    @Test
    void finalBatchTransitionsToFinalizingBeforeCommit() {
        RestoreJobExecutionContext context = runningContext();
        when(consumer.poll(any(Duration.class))).thenReturn(
                ConsumerRecords.empty(),
                records(record(0L))
        );
        when(consumer.position(TOPIC_PARTITION)).thenReturn(0L);
        when(consumer.endOffsets(java.util.Set.of(TOPIC_PARTITION))).thenReturn(Map.of(TOPIC_PARTITION, 1L));

        restoreReplicationLoop.restore(RESTORE_TYPE, null, context);

        verify(producer).beginTransaction();
        verify(producer).send(any(ProducerRecord.class));
        verify(producer).sendOffsetsToTransaction(anyMap(), eq(groupMetadata));
        verify(producer).commitTransaction();
        assertEquals(RestoreJobStatus.FINALIZING, context.getStatus());
    }

    @Test
    void cancellationBeforeFinalizationAbortsTransaction() {
        RestoreJobExecutionContext context = runningContext();
        when(producer.send(any(ProducerRecord.class))).thenAnswer(invocation -> {
            context.requestCancellation();
            return CompletableFuture.completedFuture(null);
        });
        when(consumer.poll(any(Duration.class))).thenReturn(
                ConsumerRecords.empty(),
                records(record(0L), record(1L))
        );
        when(consumer.position(TOPIC_PARTITION)).thenReturn(0L);
        when(consumer.endOffsets(java.util.Set.of(TOPIC_PARTITION))).thenReturn(Map.of(TOPIC_PARTITION, 2L));

        assertThrows(RestoreJobCancellationException.class,
                () -> restoreReplicationLoop.restore(RESTORE_TYPE, null, context));

        verify(producer).abortTransaction();
    }

    @Test
    void cancellationAfterFinalizationIsRejectedAndDoesNotAbortFinalCommit() {
        RestoreJobExecutionContext context = runningContext();
        when(consumer.poll(any(Duration.class))).thenReturn(
                ConsumerRecords.empty(),
                records(record(0L))
        );
        when(consumer.position(TOPIC_PARTITION)).thenReturn(0L);
        when(consumer.endOffsets(java.util.Set.of(TOPIC_PARTITION))).thenReturn(Map.of(TOPIC_PARTITION, 1L));

        restoreReplicationLoop.restore(RESTORE_TYPE, null, context);

        assertFalse(context.requestCancellation());
        verify(producer, never()).abortTransaction();
        assertEquals(RestoreJobStatus.FINALIZING, context.getStatus());
    }

    @Test
    void initiallyEmptyRestoreMarksFinalizingAndReturnsZeroRecords() {
        RestoreJobExecutionContext context = runningContext();
        when(consumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        when(consumer.position(TOPIC_PARTITION)).thenReturn(3L);
        when(consumer.endOffsets(java.util.Set.of(TOPIC_PARTITION))).thenReturn(Map.of(TOPIC_PARTITION, 3L));

        RestoreExecutionResult result = restoreReplicationLoop.restore(RESTORE_TYPE, null, context);

        verify(producer, never()).beginTransaction();
        assertEquals(0, result.batchesCommitted());
        assertEquals(0L, result.recordsRestored());
        assertEquals(RestoreJobStatus.FINALIZING, context.getStatus());
    }

    @Test
    void createsMultipleTransactionsForMultiplePollBatches() {
        RestoreJobExecutionContext context = runningContext();
        when(consumer.poll(any(Duration.class))).thenReturn(
                ConsumerRecords.empty(),
                records(record(0L), record(1L)),
                records(record(2L))
        );
        when(consumer.position(TOPIC_PARTITION)).thenReturn(0L);
        when(consumer.endOffsets(java.util.Set.of(TOPIC_PARTITION))).thenReturn(Map.of(TOPIC_PARTITION, 3L));

        RestoreExecutionResult result = restoreReplicationLoop.restore(RESTORE_TYPE, null, context);

        verify(producer, times(2)).beginTransaction();
        assertEquals(2, result.batchesCommitted());
        assertEquals(3L, result.recordsRestored());
    }

    private RestoreJobExecutionContext runningContext() {
        RestoreJobExecutionContext context = new RestoreJobExecutionContext(UUID.randomUUID());
        context.markRunning();
        return context;
    }

    @SafeVarargs
    private final ConsumerRecords<String, byte[]> records(ConsumerRecord<String, byte[]>... records) {
        Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> recordsByPartition = new java.util.LinkedHashMap<>();
        for (ConsumerRecord<String, byte[]> record : records) {
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            recordsByPartition.computeIfAbsent(partition, ignored -> new java.util.ArrayList<>()).add(record);
        }
        return new ConsumerRecords<>(recordsByPartition);
    }

    private ConsumerRecord<String, byte[]> record(long offset) {
        return new ConsumerRecord<>(
                SOURCE_TOPIC,
                0,
                offset,
                "key-" + offset,
                ("value-" + offset).getBytes()
        );
    }
}
