#!/usr/bin/env bash
set -euo pipefail

TOPIC="${1:-application-restore-source}"
COUNT="${2:-250}"
BROKER="${3:-localhost:9092}"
CONTAINER="${KAFKA_CONTAINER_NAME:-kafka-local}"

if ! [[ "$COUNT" =~ ^[0-9]+$ ]] || [[ "$COUNT" -le 0 ]]; then
  echo "COUNT must be a positive integer" >&2
  exit 1
fi

tmp_file="$(mktemp)"
trap 'rm -f "$tmp_file"' EXIT

for i in $(seq 1 "$COUNT"); do
  timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  printf 'mock-key-%04d:{"messageId":"msg-%04d","entityType":"application","sequence":%d,"createdAt":"%s","payload":{"applicantId":"applicant-%04d","documentId":"document-%04d","status":"RESTORE_TEST","notes":"mock restore payload %04d"}}\n' \
    "$i" "$i" "$i" "$timestamp" "$i" "$i" "$i" >> "$tmp_file"
done

podman exec -i "$CONTAINER" /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server "$BROKER" \
  --topic "$TOPIC" \
  --property parse.key=true \
  --property key.separator=: < "$tmp_file"

echo "Seeded $COUNT mock messages into $TOPIC via $BROKER"
