#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

podman compose up -d

echo "Waiting for kafka-local to become healthy..."
for _ in $(seq 1 30); do
  status="$(podman inspect -f '{{.State.Health.Status}}' kafka-local 2>/dev/null || true)"
  if [[ "$status" == "healthy" ]]; then
    break
  fi
  sleep 2
done

status="$(podman inspect -f '{{.State.Health.Status}}' kafka-local)"
if [[ "$status" != "healthy" ]]; then
  echo "kafka-local did not become healthy" >&2
  exit 1
fi

echo "Waiting for topic bootstrap container to finish..."
for _ in $(seq 1 30); do
  init_state="$(podman inspect -f '{{.State.Status}} {{.State.ExitCode}}' kafka-restore-job_kafka-init_1 2>/dev/null || true)"
  if [[ "$init_state" == "exited 0" ]]; then
    break
  fi
  sleep 1
done

init_state="$(podman inspect -f '{{.State.Status}} {{.State.ExitCode}}' kafka-restore-job_kafka-init_1)"
if [[ "$init_state" != "exited 0" ]]; then
  echo "topic bootstrap container did not finish successfully: $init_state" >&2
  podman logs kafka-restore-job_kafka-init_1 >&2 || true
  exit 1
fi

echo "Kafka broker is healthy. Current topics:"
podman exec kafka-local /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
