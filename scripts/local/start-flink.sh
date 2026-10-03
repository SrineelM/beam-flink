#!/usr/bin/env bash
# Start local standalone Flink cluster
set -e

FLINK_DIR="data/flink"
if [ ! -d "$FLINK_DIR" ]; then
    echo "Flink not installed. Run ./scripts/local/install-flink.sh first."
    exit 1
fi

"$FLINK_DIR/bin/start-cluster.sh"
echo "Flink cluster started. Web UI available at http://localhost:8081"
