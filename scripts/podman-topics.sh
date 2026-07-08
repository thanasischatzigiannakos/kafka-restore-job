#!/usr/bin/env bash
set -euo pipefail

podman exec kafka-local /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
