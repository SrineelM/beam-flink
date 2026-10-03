#!/usr/bin/env bash
# Script to download and set up Kafka 4.3.1 locally (KRaft mode) without Docker
set -e

KAFKA_VERSION="4.3.1"
SCALA_VERSION="2.13"
TARGET_DIR="data/kafka"

if [ -d "$TARGET_DIR" ]; then
    echo "Kafka already installed in $TARGET_DIR"
    exit 0
fi

echo "Downloading Kafka $KAFKA_VERSION..."
mkdir -p data
curl -sO "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
tar -xzf "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
mv "kafka_${SCALA_VERSION}-${KAFKA_VERSION}" "$TARGET_DIR"
rm "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"

echo "Kafka $KAFKA_VERSION installed successfully to $TARGET_DIR."
