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
resolve one RestorePayloadHandler for that restoreType
    ↓
parse payload bytes inside the handler via parseFrom(byte[])
    ↓
validate that the payload matches the expected logical type
    ↓
extract populated file references, if that restore type has any
    ↓
for each extracted object key, run S3 headObject against the single configured bucket
    ↓
if validation succeeds, forward the original bytes unchanged
```

If a restore type can contain file information but a specific message does not currently contain any populated file fields, validation still succeeds. In that case the handler returns an empty file-reference list and no S3 call is made.

## Main classes

- `RestoreReplicationLoop`
  Owns the transactional Kafka consume/validate/produce loop.

- `RestorePayloadValidator`
  Delegates record validation to the restore-type handler and runs S3 existence checks for extracted references.

- `RestorePayloadHandlerRegistry`
  Maps `restoreType` to exactly one handler.

- `ApplicationRestorePayloadHandler`
  Unpacks `ApplicationRestoreMessage`, validates `entityType == "application"`, and extracts file object keys.

- `AbuseRestorePayloadHandler`
  Unpacks `AbuseRestoreMessage`, validates that the message has the expected abuse shape, and extracts file object keys.

- `NotificationRestorePayloadHandler`
  Unpacks `NotificationRestoreMessage` and validates `entityType == "notification"`.

- `GenericRestoreTransformer`
  Copies the source record bytes unchanged to the target topic and adds a `restore-message-type` header with the restore type.

- `S3FileExistenceVerifier`
  Uses `headObject` against the configured bucket to confirm that each extracted object key exists.

## Kafka transaction behavior

Validation runs inside the same thread and the same transaction scope as production.

For each batch:

1. `beginTransaction()`
2. validate record 1
3. send record 1
4. validate record 2
5. send record 2
6. send source offsets to the transaction
7. `commitTransaction()`

If validation or send fails at any point:

1. the current transaction is aborted
2. no source offsets are committed for that batch
3. no partial batch is visible to `read_committed` consumers

## Pipeline configuration

Kafka pipelines are configured only by pipeline key plus topic/group settings:

```properties
engine.kafka.pipelines.application.source-topic=application-restore-source
engine.kafka.pipelines.application.target-topic=application-restore-target
engine.kafka.pipelines.application.group-id=application-restore-group
engine.kafka.pipelines.application.transactional-id=application-restore-tx-producer
engine.kafka.pipelines.application.batch-size=100
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

Only object existence is checked. Checksum validation is not part of the current implementation.

## Restore flow summary

1. A caller starts a restore for a configured `restoreType`.
2. `RestoreJobCoordinator` resolves the pipeline by that key.
3. `RestoreReplicationLoop` creates the Kafka consumer and producer.
4. The loop seeks to the starting offsets or committed offsets, depending on the resume flag.
5. The loop captures a fixed restore boundary using Kafka end offsets.
6. Each polled record is validated before it is sent.
7. The original serialized bytes are produced unchanged to the target topic.
8. Source offsets are committed in the same Kafka transaction as the produced batch.
