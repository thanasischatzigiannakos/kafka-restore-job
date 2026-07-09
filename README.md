# Kafka Restore Engine

Java 17 Spring Boot restore engine that restores Kafka source topics into Kafka target topics using batch-level Kafka transactions.

## Restore model

The frontend starts a restore by sending a `restoreType`, for example:

- `application`
- `abuse`

The restore type resolves:

- the Kafka pipeline to use
- the source topic
- the target topic
- the stable consumer `group.id`
- the stable producer `transactional.id`
- the batch size
- the message type
- the transformer implementation
- the future verification/check implementation

The controller does not hardcode topics. It resolves the configured pipeline from `engine.kafka.pipelines.<restoreType>.*`.

## End-To-End Runtime Flow

The runtime flow is intentionally split into clear stages:

1. frontend calls `POST /api/restores` with a `restoreType`
2. the API creates a persistent restore job row and returns a job id
3. a background worker resolves the Kafka pipeline for that restore type
4. the worker consumes a batch from the source topic
5. each Kafka message is unpacked from raw `byte[]` into a typed domain object
6. the typed object is inspected for nested binary-document references
7. each discovered binary reference is verified in S3
8. only verified records are produced to the target topic
9. source offsets are committed with `sendOffsetsToTransaction(...)`
10. the Kafka transaction is committed for the batch
11. after the full restore finishes, ZooKeeper state can be updated

Kafka IO stays byte-based.
Deserialization, nested-field traversal, and binary verification are separate layers.

## UI And Operational APIs

A minimal admin UI is served at:

```text
/
```

It can:

- start a restore job
- preview source-topic messages
- preview target-topic messages
- list recent restore jobs
- request cancellation for a running job

Main APIs:

- `POST /api/restores`
- `GET /api/jobs`
- `GET /api/jobs/{jobId}`
- `POST /api/jobs/{jobId}/cancel`
- `GET /api/pipelines/{restoreType}/messages?topic=source|target&limit=10`

## Job Lifecycle

Restore execution is now asynchronous.

When a restore starts:

- the API creates a job id
- the job is persisted in the database
- the API returns immediately with `202 Accepted`
- the actual Kafka restore runs in a background executor

Job states:

- `PENDING`
- `RUNNING`
- `CANCELLATION_REQUESTED`
- `FINALIZING`
- `CANCELLED`
- `COMPLETED`
- `FAILED`

Stored metadata includes:

- job id
- restore type
- status
- requested timestamp
- restore-from timestamp
- started timestamp
- completed timestamp
- cancellation requested timestamp
- source topic
- target topic
- message type
- restored record count
- committed batch count
- error message

The default local database is H2:

```text
jdbc:h2:file:./data/restore-engine
```

The H2 console is available at:

```text
/h2-console
```

## Cancellation Semantics

Cancellation is cooperative, not forceful.

The running loop checks for cancellation:

- before polling the next batch
- before sending each record inside the current batch

If cancellation is requested before a batch commits:

- the current Kafka transaction is aborted
- no partial batch becomes visible
- source offsets for that batch are not committed
- the job transitions to `CANCELLED`

If cancellation is requested after a batch has already committed:

- that committed batch remains valid
- the loop stops before the next batch

This preserves the exactly-once contract because offsets are still committed only through the Kafka transaction.

Only one active job is allowed at a time. Active job state and cancellation decisions are kept in memory; the database is an audit/logging resource for job lifecycle events and metadata.

The restore request accepts an optional `restoreFromTimestamp` ISO-8601 timestamp:

```json
{
  "restoreType": "application",
  "restoreFromTimestamp": "2026-07-09T10:00:00Z"
}
```

When present, Kafka `offsetsForTimes` is used to find the starting offset for each source partition. If Kafka has no offset for the timestamp on a partition, that partition is restored from the beginning. If no timestamp is provided, restoration also starts from the beginning.

## Typed Message Unpacking

The restore loop does not deserialize Kafka messages directly into transformer classes.
Instead it resolves a dedicated unpacker by `messageType`.

Main classes:

- `serialization/RestoreMessageUnpacker`
- `serialization/AbstractJsonRestoreMessageUnpacker`
- `serialization/RestoreMessageUnpackerResolver`
- `serialization/ApplicationRestoreMessageUnpacker`
- `serialization/AbuseRestoreMessageUnpacker`

Responsibilities:

- `RestoreMessageUnpacker`
  Defines the contract for converting raw Kafka `byte[]` payloads into typed objects.
- `AbstractJsonRestoreMessageUnpacker`
  Shared Jackson-based implementation for JSON payloads.
- `RestoreMessageUnpackerResolver`
  Maps `messageType` to the correct unpacker implementation.
- `ApplicationRestoreMessageUnpacker`
  Deserializes application restore messages.
- `AbuseRestoreMessageUnpacker`
  Deserializes abuse restore messages.

The current example uses internal-model stand-ins:

- `serialization/model/ApplicationRestoreMessage`
- `serialization/model/AbuseRestoreMessage`

In the real project, those stand-ins should be replaced by the actual classes from your internal libraries. The unpackers are the seam where that replacement happens cleanly.

## Binary Reference Extraction

Nested binary objects are not discovered by the Kafka loop itself.
That logic lives in message-type-specific extractors.

Main classes:

- `verification/BinaryReference`
- `verification/BinaryReferenceExtractor`
- `verification/BinaryReferenceExtractorResolver`
- `verification/ApplicationBinaryReferenceExtractor`
- `verification/AbuseBinaryReferenceExtractor`

Responsibilities:

- `BinaryReference`
  A normalized description of a binary location to verify. It carries:
  `fieldPath`, `bucketKey`, `objectKey`, and `checksum`.
- `BinaryReferenceExtractor`
  Contract for walking a typed payload and yielding the binary references it contains.
- `BinaryReferenceExtractorResolver`
  Resolves the correct extractor from `messageType`.
- `ApplicationBinaryReferenceExtractor`
  Knows where application payloads may contain documents such as:
  `writtenDocument`, `applicant.uploadedFiles`, `translatedFiles`, `signedForm`, `signedLocallyForm`.
- `AbuseBinaryReferenceExtractor`
  Knows where abuse payloads may contain documents such as:
  `uploadedFile` and `attachments`.

This is the clean place to encode knowledge of nested structures from your internal DTOs.
The core Kafka loop should not know where those fields live.

## Binary Verification

Main classes:

- `verification/BinaryVerificationService`
- `s3/S3BucketResolver`
- `s3/S3ClientFactory`

Responsibilities:

- `BinaryVerificationService`
  Resolves the extractor for the current `messageType`, extracts all binary references from the typed payload, groups S3 access by bucket configuration, and verifies that each referenced object exists.
- `S3BucketResolver`
  Maps a logical `bucketKey` to concrete S3 bucket configuration.
- `S3ClientFactory`
  Creates S3 clients from configured endpoint, region, credentials, and path-style settings.

The current implementation verifies existence with `HeadObject`.
Checksum comparison remains a future layer and should be added after the message-specific checksum extraction rules are finalized.

## Why transactions are required

Exactly-once recovery is required because duplicates are not acceptable.

The producer is transactional and must use a stable `transactional.id` across restarts.
The consumer must use a stable `group.id` across restarts.
`enable.auto.commit=false` is mandatory.
Source offsets are committed only with `sendOffsetsToTransaction(...)`.
The produce path is single-threaded to preserve ordering.
One Kafka transaction is used per batch.
The whole restore job must not be wrapped in one Kafka transaction.

If a mid-batch failure happens:

- the batch transaction is aborted
- no partial batch is committed
- the next run resumes from the last committed source offsets

All downstream consumers of the target topic must use:

```properties
isolation.level=read_committed
```

Otherwise aborted batches may become visible.

## Configuration

All properties live under `engine.*`.

Main groups:

- `engine.kafka`
- `engine.s3`
- `engine.zookeeper`
- `engine.verification`

Kafka supports multiple pipelines. Two examples are configured by default:

- `application`
- `abuse`

Example pipeline properties:

```properties
engine.kafka.pipelines.application.source-topic=${ENGINE_KAFKA_PIPELINE_APPLICATION_SOURCE_TOPIC:application-restore-source}
engine.kafka.pipelines.application.target-topic=${ENGINE_KAFKA_PIPELINE_APPLICATION_TARGET_TOPIC:application-restore-target}
engine.kafka.pipelines.application.group-id=${ENGINE_KAFKA_PIPELINE_APPLICATION_GROUP_ID:application-restore-group}
engine.kafka.pipelines.application.transactional-id=${ENGINE_KAFKA_PIPELINE_APPLICATION_TRANSACTIONAL_ID:application-restore-tx-producer}
engine.kafka.pipelines.application.batch-size=${ENGINE_KAFKA_PIPELINE_APPLICATION_BATCH_SIZE:100}
engine.kafka.pipelines.application.message-type=${ENGINE_KAFKA_PIPELINE_APPLICATION_MESSAGE_TYPE:application}
```

Kafka security properties are prepared for:

- `PLAINTEXT`
- `SSL`
- `SASL_SSL`
- `SASL_PLAINTEXT`

## Class Responsibilities

Core runtime classes:

- `api/RestoreJobController`
  REST entrypoints for starting restores, listing jobs, cancelling jobs, and previewing messages.
- `job/RestoreJobCoordinator`
  Owns asynchronous job lifecycle, persistence updates, active-job guards, and cooperative cancellation.
- `kafka/RestoreReplicationLoop`
  Owns the batch consume-unpack-verify-transform-produce transaction loop.
- `kafka/RestoreTransformer`
  Contract for producing target Kafka records from source Kafka records.
- `kafka/ApplicationRestoreTransformer`
  Application-specific producer-record mapping.
- `kafka/AbuseRestoreTransformer`
  Abuse-specific producer-record mapping.
- `config/KafkaClientConfiguration`
  Builds transactional producers, restore consumers, and preview consumers.
- `config/KafkaSecurityConfigHelper`
  Applies optional PLAINTEXT, SSL, SASL_SSL, and SASL_PLAINTEXT settings.
- `job/RestoreJobEntity`
  Persistent record of job status, timing, and outcome metadata.
- `job/RunningRestoreJob`
  In-memory handle for cancellation state of an active background execution.
- `service/KafkaMessagePreviewService`
  Reads bounded message samples from source or target topics for the UI.
- `zookeeper/ZooKeeperCommandProcessorStateRepository`
  Reserved for post-restore ZooKeeper state updates only, never per batch.

## Local Kafka With Podman

This workspace has `podman` and `podman-compose`, so the repository includes helper scripts for local execution:

Start Kafka and create local example topics:

```bash
./scripts/podman-up.sh
```

List topics:

```bash
./scripts/podman-topics.sh
```

Stop the stack:

```bash
./scripts/podman-down.sh
```

Reset containers and volumes:

```bash
./scripts/podman-reset.sh
```

Seed mock restore messages into a source topic:

```bash
./scripts/seed-mock-messages.sh application-restore-source 250
```

The compose file uses separate Kafka listeners for:

- host access at `localhost:9092`
- container-to-container access at `kafka:29092`

That split is required so the topic bootstrap container can connect correctly under Podman.

## Local Kafka With Docker-Compatible Compose

If Docker is available elsewhere, the same compose file can still be used with:

```bash
docker compose up -d
```

Topics created by default:

- `topic-a-backup`
- `topic-b-primary`
- `application-restore-source`
- `application-restore-target`
- `abuse-restore-source`
- `abuse-restore-target`

## Run the app

```bash
./mvnw spring-boot:run
```

## Start a restore

Application restore:

```bash
curl -X POST http://localhost:8080/api/restores \
  -H 'Content-Type: application/json' \
  -d '{"restoreType":"application"}'
```

Abuse restore:

```bash
curl -X POST http://localhost:8080/api/restores \
  -H 'Content-Type: application/json' \
  -d '{"restoreType":"abuse"}'
```

## Package layout

```text
src/main/java/.../restoreengine
├── api
├── config
├── job
├── kafka
├── serialization
├── s3
├── verification
└── zookeeper
```

## Notes

S3 client and bucket resolution are prepared for future binary verification checks.
ZooKeeper state updates are intentionally separate from Kafka transactions and should happen only after the full restore job finishes.
