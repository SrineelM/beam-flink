# Technology Versions Rationale

> **Researched Date:** 2026-08-10

| Component | Selected Version | Reason & Rationale |
|---|---|---|
| **Java** | `17.0.x` (LTS) | LTS release. Required baseline for Flink 2.2 and Beam 2.75. |
| **Apache Beam** | `2.75.0` | Latest stable (released July 2026). Added official support for Flink 2.1/2.2 runners. |
| **Apache Flink** | `2.2.1` | Latest stable Flink 2.2 series (released May 2026). Fully compatible with Beam 2.75.0. |
| **Apache Kafka** | `4.3.1` | Latest stable (released June 2026). KRaft-only mode. Removes ZooKeeper dependency. |
| **PostgreSQL** | `17.10` | Latest minor release for PG 17 series (May 2026). Pinned version for OS stability. |
| **AWS SDK v2** | `2.26.31` | Provides S3 client used for object storage (works with S3, MinIO, GCS HMAC, R2). |
| **OpenTelemetry** | `1.44.1` | Current stable release for Java tracing SDK. |
