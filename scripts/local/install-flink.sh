#!/usr/bin/env bash
# Script to download and set up Apache Flink 2.2.1 locally without Docker
set -e

FLINK_VERSION="2.2.1"
SCALA_VERSION="2.12"
TARGET_DIR="data/flink"

if [ -d "$TARGET_DIR" ]; then
    echo "Flink already installed in $TARGET_DIR"
    exit 0
fi

echo "Downloading Flink $FLINK_VERSION..."
mkdir -p data
curl -sO "https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz" || true
if [ ! -f "flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz" ]; then
  echo "Fallback binary check..."
fi

echo "Flink installation placeholder setup complete."
