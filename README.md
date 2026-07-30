# Kafka Restore Engine

Java 17 Spring Boot restore engine that copies records from configured Kafka source topics to configured Kafka target topics using Kafka transactions.

## Current validation flow

The runtime is now keyed by the configured pipeline name, which is also the logical restore type:

- `application`
- `abuse`
- `notification`

There is no separate `pipeline.type` property anymore. The pipeline key is the source of truth for routing.

For each consumed record, the loop does this before producing to the target topic:

```text
read restoreType from the selected pipeline key
    ↓
resolve one MessageHandler for that restoreType
    ↓
parse payload bytes inside the handler via parseFrom(byte[])
    ↓
validate that the payload matches the expected logical type
    ↓
extract populated file references, if that restore type has any
    ↓
for each extracted object key, run S3 headObject against the single configured bucket
    ↓
when a checksum is present, compare it against S3 SHA-256 metadata or a streamed SHA-256 fallback
    ↓
if validation succeeds, forward the original bytes unchanged
```

If a restore type can contain file information but a specific message does not currently contain any populated file fields, validation still succeeds. In that case the handler returns an empty file-reference list and no S3 call is made.

If a record parses successfully but does not match the expected logical type for that restore flow, the loop logs the mismatch, commits that source offset transactionally, and continues with the next record. Binary existence failures remain fatal and stop the restore.

## Main classes

- `RestoreReplicationLoop`
  Owns the transactional Kafka consume/validate/produce loop.

- `MessageHandlerRegistry`
  Maps `restoreType` to exactly one handler.

- `AbstractMessageHandler`
  Common parse and structure-validation base for all handlers.

- `BinaryMessageHandler`
  Extends the common handler path with file-reference extraction and S3 existence checks.

- `ApplicationMessageHandler`
  Parses `ApplicationRestoreMessage`, validates `entityType == "application"`, and checks extracted file object keys.

- `AbuseMessageHandler`
  Parses `AbuseRestoreMessage`, validates that the message has the expected abuse shape, and checks extracted file object keys.

- `NotificationMessageHandler`
  Parses `NotificationRestoreMessage` and validates `entityType == "notification"`.

- `GenericRestoreTransformer`
  Copies the source record bytes unchanged to the target topic and adds a `restore-message-type` header with the restore type.

- `S3FileExistenceVerifier`
  Uses `headObject` against the configured bucket to confirm that each extracted object key exists and validates SHA-256 checksums when present.

## Kafka transaction behavior

Validation runs inside the same thread and the same transaction scope as production.

For each restored record:

1. `beginTransaction()`
2. validate the record payload
3. send the target record
4. send that record's next source offset to the transaction
5. `commitTransaction()`

If validation or send fails for a record:

1. that record's transaction is aborted
2. that record's source offset is not committed
3. already committed earlier records remain durably restored
4. no partial target write from the failing record is visible to `read_committed` consumers

If a record only has a message-type mismatch:

1. the loop logs the mismatch
2. no target record is produced for that source record
3. that source offset is still committed in its own producer transaction
4. the restore continues with the next record

## Pipeline configuration

Kafka pipelines are configured only by pipeline key plus topic/group settings:

```properties
engine.kafka.pipelines.application.source-topic=application-restore-source
engine.kafka.pipelines.application.target-topic=application-restore-target
engine.kafka.pipelines.application.group-id=application-restore-group
engine.kafka.pipelines.application.transactional-id=application-restore-tx-producer
```

Equivalent entries exist for `abuse` and `notification`.

## S3 configuration

The validation flow now assumes a single S3 bucket for file existence checks:

```properties
engine.s3.bucket=restore-default-bucket
engine.s3.endpoint=
engine.s3.region=eu-west-1
engine.s3.access-key=
engine.s3.secret-key=
engine.s3.path-style-access=false
```

Checksum handling:

- missing or blank checksum: existence-only validation
- checksum present: compare as SHA-256
- prefer S3 checksum metadata when available
- otherwise stream the object and calculate SHA-256 locally

## Restore flow summary

1. A caller starts a restore for a configured `restoreType`.
2. `RestoreJobCoordinator` resolves the pipeline by that key.
3. `RestoreReplicationLoop` creates the Kafka consumer and producer.
4. The loop seeks to the starting offsets or committed offsets, depending on the resume flag.
5. The loop captures a fixed restore boundary using Kafka end offsets.
6. Each polled record is validated before it is sent.
7. The original serialized bytes are produced unchanged to the target topic.
8. Source offsets are committed in the same Kafka transaction as each produced record.

## Consumer Progress

The restore consumer does not use auto-commit or `commitSync()`.

Instead, after one source record is validated and its target copy is sent successfully, the loop commits the source progress by:

1. `producer.sendOffsetsToTransaction(...)`
2. `producer.commitTransaction()`

That makes the committed consumer-group offset and the target-topic write succeed or fail together. A crash after `poll()` but before transaction commit does not advance the durable source offset.
