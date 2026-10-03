#!/usr/bin/env bash
# Stop local standalone Flink cluster
set -e

FLINK_DIR="data/flink"
if [ -d "$FLINK_DIR" ]; then
    "$FLINK_DIR/bin/stop-cluster.sh" || true
    echo "Flink cluster stopped."
fi
