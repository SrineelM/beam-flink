#!/usr/bin/env bash
# Create required Kafka topics locally with 3 partitions for laptop testing
set -e

BOOTSTRAP_SERVER="localhost:9092"
KAFKA_DIR="data/kafka"

if [ ! -d "$KAFKA_DIR" ]; then
    echo "Kafka directory not found at $KAFKA_DIR"
    exit 1
fi

TOPICS=("orders" "payments" "customer-events" "dlq" "alerts" "metrics-output" "cart-abandonment")

for TOPIC in "${TOPICS[@]}"; do
    echo "Creating topic: $TOPIC"
    "$KAFKA_DIR/bin/kafka-topics.sh" --create --if-not-exists \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --replication-factor 1 \
        --partitions 3 \
        --topic "$TOPIC"
done

echo "Kafka topics created successfully."
