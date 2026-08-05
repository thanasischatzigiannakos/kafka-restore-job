# Helm Deployment Notes

This project now includes a Helm chart at [helm/kafka-restore-job](/home/athanasioschatzigiannakos/projects/kafka-restore-job/helm/kafka-restore-job).

## Scope

The chart is intentionally simple:

- one `Deployment`
- one `Service`
- one OpenShift `Route`
- one `ConfigMap` for non-secret environment variables
- optional use of one existing secret for sensitive env vars
- optional secret mounts for source and target Kafka truststore or keystore files
- optional `PersistentVolumeClaim` for the embedded H2 database

## OpenShift namespaces and Kafka clusters

If the Kafka clusters run in different namespaces, the application namespace does not need any special Kubernetes object for that on its own. What it needs is reachable bootstrap addresses.

Typical in-cluster DNS examples:

- source cluster:
  `source-kafka-bootstrap.kafka-source.svc:9092`
- target cluster:
  `target-kafka-bootstrap.kafka-target.svc:9092`

Those are configured through:

- `ENGINE_KAFKA_SOURCE_BOOTSTRAP_SERVERS`
- `ENGINE_KAFKA_TARGET_BOOTSTRAP_SERVERS`

If your platform exposes Kafka through routes or load balancers instead, use those hostnames instead of `.svc` DNS names.

## Secrets

The chart assumes some values come from a secret rather than plain values.

### 1. Secret env values

Set:

- `secretEnv.existingSecret`

Then the deployment maps selected secret keys into env vars. By default the chart expects keys such as:

- `spring-datasource-password`
- `source-sasl-jaas-config`
- `target-sasl-jaas-config`
- `source-truststore-password`
- `source-keystore-password`
- `source-key-password`
- `target-truststore-password`
- `target-keystore-password`
- `target-key-password`
- `s3-access-key`
- `s3-secret-key`

These map into env vars such as:

- `SPRING_DATASOURCE_PASSWORD`
- `ENGINE_KAFKA_SOURCE_SASL_JAAS_CONFIG`
- `ENGINE_KAFKA_TARGET_SASL_JAAS_CONFIG`
- `ENGINE_S3_ACCESS_KEY`
- `ENGINE_S3_SECRET_KEY`

If your secret uses different key names, change `secretEnv.mappings` in `values.yaml`.

### 2. Secret-mounted truststores and keystores

If source and target Kafka clusters use TLS material stored as files, mount them from existing secrets:

- `kafkaTlsSecrets.source.existingSecret`
- `kafkaTlsSecrets.target.existingSecret`

Default mount paths:

- source: `/etc/kafka/source`
- target: `/etc/kafka/target`

Then set the matching env vars to the mounted file paths, for example:

- `ENGINE_KAFKA_SOURCE_TRUSTSTORE_LOCATION=/etc/kafka/source/truststore.p12`
- `ENGINE_KAFKA_TARGET_TRUSTSTORE_LOCATION=/etc/kafka/target/truststore.p12`

The chart does not create these secrets for you. It expects them to exist already, which keeps the chart simple and avoids embedding sensitive content into chart values.

## Security property precedence

The application currently supports:

- shared Kafka security values under `engine.kafka.*`
- source-specific overrides under `engine.kafka.source.*`
- target-specific overrides under `engine.kafka.target.*`

The chart exposes both source and target overrides directly through env vars.

Behavior:

- source consumer uses `ENGINE_KAFKA_SOURCE_*` values when set
- target producer uses `ENGINE_KAFKA_TARGET_*` values when set
- if those are blank, the app falls back to shared `ENGINE_KAFKA_*` values

## Persistence

By default the chart creates a PVC and mounts it at `/app/data`.

This is important because the current Spring datasource URL is expected to point at a file-backed H2 database:

- `jdbc:h2:file:/app/data/restore-engine;AUTO_SERVER=TRUE`

If you move to an external database later, you can disable persistence and override the datasource URL and credentials.

## Helm binary

`helm` was installed locally in this workspace as:

- [.tools-helm](/home/athanasioschatzigiannakos/projects/kafka-restore-job/.tools-helm)

You can render the chart with:

```bash
./.tools-helm template test ./helm/kafka-restore-job
```
