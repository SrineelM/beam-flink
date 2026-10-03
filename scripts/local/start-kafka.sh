#!/usr/bin/env bash
# Start local Kafka in KRaft mode
set -e

KAFKA_DIR="data/kafka"
if [ ! -d "$KAFKA_DIR" ]; then
    echo "Kafka not installed. Run ./scripts/local/install-kafka.sh first."
    exit 1
fi

CLUSTER_ID=$("$KAFKA_DIR/bin/kafka-storage.sh" random-cluster-id)
"$KAFKA_DIR/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$KAFKA_DIR/config/kraft/server.properties" --ignore-formatted
"$KAFKA_DIR/bin/kafka-server-start.sh" -daemon "$KAFKA_DIR/config/kraft/server.properties"
echo "Kafka KRaft started on localhost:9092"
