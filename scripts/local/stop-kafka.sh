#!/usr/bin/env bash
# Stop local Kafka broker
set -e

KAFKA_DIR="data/kafka"
if [ -d "$KAFKA_DIR" ]; then
    "$KAFKA_DIR/bin/kafka-server-stop.sh" || true
    echo "Kafka stopped."
fi
