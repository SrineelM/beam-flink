# Laptop Constraints & Dual-Target Strategy

This codebase is architected for dual-target execution:
1. **Local Developer Laptop:** 8GB RAM Mac, process-based local services (Homebrew), no Docker requirement.
2. **Enterprise Production:** Kubernetes cluster, Apache Flink Kubernetes Operator, 50,000 EPS target.

## Summary of Constraints & Solutions

| Feature | Local Laptop Strategy | Enterprise Production Upgrade |
|---|---|---|
| **Containerization** | Direct Java execution via Maven / shell scripts | Docker multi-stage builds (`infrastructure/docker/`) |
| **Cluster Orchestration** | Local standalone Flink / DirectRunner | Apache Flink Kubernetes Operator 1.15.x (`infrastructure/kubernetes/`) |
| **Object Storage** | Local filesystem directory (`./data/events`) | AWS S3 / GCS bucket via S3 API compatibility |
| **Kafka Execution** | Single-broker KRaft process (`scripts/local/start-kafka.sh`) | 3+ Broker Kafka Cluster with 12 partitions per topic |
| **Observability** | Console log export / Flink Web UI (port 8081) | Prometheus scraping + Grafana dashboards (`monitoring/`) |
| **Target Scale** | 500 – 1,000 events/sec | 50,000+ events/sec |
