# Kafka Restore Engine

Java 17 Spring Boot restore engine that copies records from configured Kafka source topics to configured Kafka target topics using Kafka transactions.

The current implementation tracks restore jobs in memory. It does not persist job state to the database during execution, and it does not update ZooKeeper as part of the active restore flow.

## Runtime Summary

At a high level, the system does four things:

1. Accept a restore request for a configured `restoreType`.
2. Start an asynchronous background job.
3. Read source-topic records up to a fixed restore boundary.
4. Write each consumed batch into the target topic inside its own Kafka transaction.

The main goals of the runtime design are:

- atomic batch-level commit of target records and source offsets
- single-threaded produce path
- safe cancellation while the job is still `RUNNING`
- deterministic completion based on a fixed Kafka end-offset snapshot

## Message Validation Approach

Before a consumed record is sent to the target topic, the restore loop validates it against the configured pipeline `messageType`.

The current implementation supports two validation paths:

- file-capable message types
  These are message types whose payloads may contain file-related data and therefore must be unpacked before the record is forwarded.
- type-check-only message types
  These are message types that do not use the file-capable path. They are validated by a dedicated checker for that configured message type.

The distinction is explicit.

- `application` and `abuse` are registered as file-capable payload classes.
- `notification` is the example type-check-only message type.

### Current supporting classes

- `ConfiguredPayloadTypeResolver`
  Maps configured pipeline `messageType` values such as `application`, `abuse`, and `notification` to the Java payload class used for JSON unpacking.

- `FileCapablePayloadRegistry`
  Holds the explicit set of payload classes that should be unpacked on the restore path because they may contain file-related content.

- `ExpectedMessageTypeCheckerRegistry`
  Holds the explicit set of type-check-only validators keyed by configured `messageType`.

- `RestorePayloadValidator`
  Chooses which validation path to use for each consumed record.

### Validation flow

For every consumed Kafka record, the loop now follows this path:

```text
read configured pipeline.messageType
    ↓
resolve expected payload class
    ↓
is the payload class registered as file-capable?
    ├── yes → unpack JSON payload into the expected class
    │         ↓
    │     run optional message-type checker
    └── no  → run the configured message-type checker
```

This version intentionally stops at unpacking or type checking.
It does not perform S3 lookups, checksum checks, or binary completeness validation.
If a file-capable message does not actually include file data, the loop still continues as long as
the payload can be unpacked and any configured type check passes.

### Example type-only message

The repository includes an example non-file-bearing message:

- `notification`

It uses:

- `NotificationRestoreMessage`
- `NotificationRestoreMessageUnpacker`
- `NotificationExpectedMessageTypeChecker`
- `NotificationRestoreTransformer`

The checker unpacks the message and verifies that `entityType == "notification"`.
That makes the type-only branch concrete instead of being a no-op.

## Request Flow

### Start restore

The frontend or caller sends:

```http
POST /api/restores
```

with a body such as:

```json
{
  "restoreType": "application",
  "restoreFromTimestamp": "2026-07-09T10:00:00Z"
}
```

Flow:

1. `RestoreJobController` receives the request.
2. `RestoreJobCoordinator.startJob(...)` validates that the `restoreType` exists in Kafka configuration.
3. The coordinator creates:
   - a new `jobId`
   - a `RestoreJobExecutionContext` for atomic lifecycle transitions
   - an `InMemoryRestoreJob` that holds request/result metadata for API responses
4. The job is stored in the coordinator's in-memory maps.
5. The coordinator submits the actual work to `restoreJobExecutor`.
6. The API immediately returns `202 Accepted` with the job id.

### Query job state

The frontend can call:

```http
GET /api/jobs
GET /api/jobs/{jobId}
```

These endpoints read directly from the coordinator's in-memory job store and return `RestoreJobResponse`.

### Cancel a running job

The frontend can call:

```http
POST /api/jobs/{jobId}/cancel
```

Flow:

1. `RestoreJobCoordinator.requestCancellation(...)` looks up the in-memory job.
2. It calls `RestoreJobExecutionContext.requestCancellation()`.
3. That transition is atomic and only succeeds from:
   - `PENDING`
   - `RUNNING`
4. Cancellation is rejected from:
   - `FINALIZING`
   - `COMPLETED`
   - `CANCELLED`
   - `FAILED`

## Main Runtime Flow

Once the background task starts, the coordinator executes:

1. `context.markRunning()`
2. resolve `EngineKafkaProperties.PipelineProperties` from the configured `restoreType`
3. `RestoreReplicationLoop.restore(...)`
4. If the loop returns successfully:
   - the in-memory job records the `RestoreExecutionResult`
   - the context transitions `FINALIZING -> COMPLETED`
5. If cancellation is raised:
   - the context transitions `CANCELLATION_REQUESTED -> CANCELLED`
6. If any other runtime failure happens:
   - the context transitions to `FAILED`

Only one active restore job is allowed at a time.

## What Happens Inside `RestoreReplicationLoop`

`RestoreReplicationLoop` is the core Kafka execution engine.

Its responsibilities are:

- create Kafka consumer and producer instances for the selected pipeline
- seek the consumer to the correct starting offsets
- capture a fixed restore boundary using Kafka end offsets
- poll source records
- discard records outside the fixed boundary
- write each non-empty batch transactionally to the target topic
- commit source offsets through the same Kafka transaction
- move the job to `FINALIZING` before the final `commitTransaction()`

### Step-by-step loop behavior

#### 1. Resolve pipeline and create Kafka clients

The coordinator resolves the configured pipeline from `restoreType` and passes that
`PipelineProperties` object into the loop. The loop then opens:

- a transactional `KafkaProducer<String, byte[]>`
- a `KafkaConsumer<String, byte[]>`

#### 2. Assign partitions and seek to the starting point

`seekToStartingOffsets(...)` does the following:

- waits for partition assignment
- if `restoreFromTimestamp` is `null`, seeks to the beginning
- otherwise calls `offsetsForTimes(...)`
- if Kafka has no matching offset for a partition, seeks that partition to the beginning

#### 3. Capture a fixed restore boundary

After assignment and seeking, the loop calls:

```java
consumer.endOffsets(consumer.assignment())
```

These end offsets are exclusive.

Example:

- end offset `250`
- last restorable record offset `249`

Records produced to the source topic after this snapshot are intentionally excluded from the current restore run.

#### 4. Handle the zero-record edge case

The loop compares current consumer positions with the captured end offsets.

If all current positions are already at or beyond the boundary:

- the job transitions `RUNNING -> FINALIZING`
- no Kafka transaction is started
- an empty `RestoreExecutionResult` is returned

This is the only successful restore path without a transaction, because there are no records to write and no offsets to commit.

#### 5. Poll records

The loop then repeatedly:

1. checks for cancellation
2. polls Kafka
3. handles empty polls
4. filters records that are still inside the restore boundary

Empty polls are not the primary completion signal.
Normal completion is driven by reaching the fixed end-offset snapshot.

Empty polls are only used as a safety guard so the loop does not wait forever if Kafka behaves unexpectedly.

#### 6. Filter records against the fixed boundary

For every poll batch, records are kept only if:

```java
record.offset() < restoreEndOffsets.get(topicPartition)
```

Records at or above the captured boundary are ignored for this run.

#### 7. Restore one batch in one Kafka transaction

For every non-empty restorable batch, `restoreBatch(...)` does:

1. `producer.beginTransaction()`
2. validate the source record payload using the configured pipeline
3. sequentially send every record to the target topic
4. calculate the next source offsets
5. `producer.sendOffsetsToTransaction(...)`
6. if this is the final batch:
   - `RUNNING -> FINALIZING`
7. `producer.commitTransaction()`

If any send, offset submission, or commit fails:

- the loop attempts `abortTransaction()`
- the batch fails
- the exception propagates back to the coordinator

If payload validation fails:

- the current transaction is aborted
- the failing record is not sent
- later records in the batch are not processed
- source offsets are not committed

#### 8. Detect the final batch

The loop keeps `restoredPositions` per partition.

After calculating batch offsets, it updates positions using the next offset to consume:

```java
lastProcessedOffset + 1
```

The batch is final when all restored positions reach the captured end offsets.

That means completion is deterministic and based on Kafka positions, not on timing or poll count.

## Transaction Model

The restore engine uses one Kafka transaction per consumed batch.

Important properties:

- produce path is single-threaded
- records are sent in order
- source offsets are committed only through `sendOffsetsToTransaction(...)`
- target writes and source-offset commits succeed or fail together

This means:

- no partial batch is visible if a failure happens mid-batch
- no partial batch offset is committed
- the next run resumes from the last committed source position

The implementation does not wrap the whole restore job in one long Kafka transaction.

## Cancellation Model

Cancellation is cooperative and race-safe.

The critical race is:

- `RUNNING -> CANCELLATION_REQUESTED`
- `RUNNING -> FINALIZING`

Both are atomic compare-and-set transitions in `RestoreJobExecutionContext`.
Only one can win.

### If cancellation wins

- the current batch aborts
- the job becomes `CANCELLED`

### If finalizing wins

- cancellation is rejected
- the final transaction is allowed to commit
- the job later becomes `COMPLETED`

There is intentionally no cancellation check after a successful `RUNNING -> FINALIZING` transition.

## Job States

The in-memory lifecycle is:

- `PENDING`
- `RUNNING`
- `CANCELLATION_REQUESTED`
- `FINALIZING`
- `CANCELLED`
- `COMPLETED`
- `FAILED`

Valid transitions:

- `PENDING -> RUNNING`
- `PENDING -> CANCELLATION_REQUESTED`
- `RUNNING -> CANCELLATION_REQUESTED`
- `RUNNING -> FINALIZING`
- `CANCELLATION_REQUESTED -> CANCELLED`
- `FINALIZING -> COMPLETED`
- `PENDING/RUNNING/CANCELLATION_REQUESTED/FINALIZING -> FAILED`

## Main Classes And Responsibilities

### API layer

- `RestoreJobController`
  Exposes REST endpoints for starting restores, listing jobs, fetching a job, cancelling a job, and previewing Kafka messages.

### Job orchestration

- `RestoreJobCoordinator`
  Owns the in-memory job registry, enforces single active job execution, starts background work, handles lifecycle transitions around the replication loop, and serves job responses to the API.

- `RestoreJobExecutionContext`
  The atomic in-memory lifecycle state machine. It owns only the job id, current status, and cancellation timestamp.

- `InMemoryRestoreJob`
  The coordinator-owned in-memory snapshot used for API responses. It stores request metadata, timestamps, result data, failure message, and a reference to the execution context.

- `RestoreJobResponse`
  API-facing DTO built from `InMemoryRestoreJob`.

- `RestoreJobStatus`
  Enum for the lifecycle states.

- `RestoreJobCancellationException`
  Signals cooperative cancellation from the loop back to the coordinator.

- `RestoreJobStateException`
  Signals an invalid lifecycle transition.

### Kafka restore engine

- `RestoreReplicationLoop`
  Runs the restore itself: seek, boundary capture, polling, per-record payload validation, filtering, transactional batch commit, final-batch detection, and transaction abort handling.

- `KafkaClientConfiguration`
  Creates configured Kafka consumers and producers for each restore pipeline.

- `KafkaOffsetCalculator`
  Computes the next source offsets to commit for the processed records.

- `RestoreExecutionResult`
  Summary of a completed restore run, including source topic, target topic, batch count, and restored record count.

- `RestorePayloadValidator`
  Selects the validation path for each record and performs either file-capable unpacking or configured type checking.

- `ConfiguredPayloadTypeResolver`
  Resolves the configured pipeline `messageType` to the Java payload class.

- `FileCapablePayloadRegistry`
  Declares which payload classes should be unpacked on the restore path.

- `ExpectedMessageTypeCheckerRegistry`
  Declares which message types have explicit type-check logic beyond plain unpacking.

### Configuration

- `EngineKafkaProperties`
  Holds Kafka-level settings and the configured `restoreType -> pipeline` mapping.

The default example pipelines are:

- `application`
- `abuse`
- `notification`

The `notification` pipeline is included as the example type-check-only path.

### Preview support

- `KafkaMessagePreviewService`
  Supports the preview endpoints for source and target topic inspection.

## Kafka Configuration Requirements

The producer must be configured with:

```properties
enable.idempotence=true
acks=all
transactional.id=<stable restore producer identity>
```

The consumer must be configured with:

```properties
enable.auto.commit=false
isolation.level=read_committed
```

Target-topic consumers should also use:

```properties
isolation.level=read_committed
```

The restore engine already preserves these assumptions in `KafkaClientConfiguration`.

## Current Persistence Note

The project still contains JPA entities and repository classes from the earlier design, but the current restore flow does not use them as part of job execution.

Right now:

- job lifecycle is in memory
- job query responses are in memory
- cancellation is in memory
- restore completion metadata is in memory

If persistent audit/history is reintroduced later, that should be added deliberately around the current coordinator and execution-context design rather than replacing the lifecycle state machine.
