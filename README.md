# Enterprise Real-Time E-Commerce Order Intelligence Platform

**Apache Beam 2.75.0 · Apache Flink 2.2.1 · Apache Kafka 4.3.1 · Java 17**

A production-grade, multi-module Apache Beam streaming pipeline that processes real-time e-commerce events from Kafka, performs stateful fraud detection and revenue aggregation, and persists results to PostgreSQL, Kafka output topics, and object storage. Designed to run on an **8 GB developer Mac without Docker** and scale to **50,000 events/second on an enterprise Kubernetes cluster** without code changes.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Module Structure](#module-structure)
- [Pipeline DAG](#pipeline-dag)
- [API & Swagger Documentation](#api--swagger-documentation)
- [Quick Start (Local)](#quick-start-local)
- [Running the Tests](#running-the-tests)
- [Configuration Reference](#configuration-reference)
- [Key Design Decisions](#key-design-decisions)
- [Technology Versions](#technology-versions)
- [Enterprise Deployment](#enterprise-deployment)

---

## Architecture Overview

```
Kafka Topics                   Apache Beam Pipeline (Flink Runner)              Sinks
─────────────────    ─────────────────────────────────────────────    ─────────────────────
orders            ──►  ParseEventFn   ──► ValidateEventFn             DLQ ──► PostgreSQL
payments          ──►  (JSON → model)     (biz rules)                       ──► Kafka dlq
customer-events   ──►      │                   │
                           │ (valid)           │ (invalid → DLQ)
                           ▼
                   AssignEventTimestampFn  (event-time watermarking)
                           │
                   DeduplicateEventsFn    (stateful TTL 24h, BagState)
                           │
                   EnrichOrderFn          (product catalog side input)
                           │
                    ┌──────┴──────┐
                    │             │
             TumblingWindow    CustomerKeyedEvents
             5-min combiner    (per-customer KV stream)
                    │                   │
          RevenueAggregateCombineFn  CustomerRiskDetectorFn   (CombiningState fraud)
                    │                   │
          AggregatedMetric         RiskSignal               SessionWindow (30-min gap)
                    │                   │                        │
                    └────────┬──────────┘                CartAbandonmentFn
                             │                                   │
                        Sinks (per record type):
                         ├── PostgresSinkDoFn  (HikariCP JDBC pool)
                         ├── KafkaSinkDoFn     (native Kafka producer)
                         └── ObjectStorageSinkWrapper (local dir / S3)
```

---

## Module Structure

```
beam-flink/
├── platform-core/          # Shared models, config, utilities
│   └── src/main/java/com/enterprise/beam/
│       ├── model/          # BaseEvent hierarchy (10 event types), DlqEnvelope, RiskSignal, AggregatedMetric
│       ├── config/         # PipelineConfig (env-var driven, dual-target defaults)
│       └── util/           # JsonUtils, StructuredLogger, RetryUtil, IdempotencyKeyBuilder
│
├── pipeline/               # Apache Beam streaming pipeline
│   └── src/main/java/com/enterprise/beam/
│       ├── pipeline/       # StreamingPipeline.java (main entry point, full DAG assembly)
│       ├── transforms/     # All DoFns and CombineFns (8 transforms)
│       └── sinks/          # PostgresSinkDoFn, KafkaSinkDoFn, ObjectStorageSinkWrapper
│
├── simulator/              # Synthetic event generator (Picocli CLI)
│   └── src/main/java/com/enterprise/beam/simulator/
│       ├── Simulator.java          # Main entry, rate-limited event loop
│       ├── EventFactory.java       # Generates all 10 event types
│       ├── OrderStateMachine.java  # Stateful order lifecycle (CREATED → DELIVERED)
│       ├── KafkaProducerWrapper.java
│       └── SimulatorConfig.java    # CLI options (--rate, --events, --duration, ...)
│
├── scripts/
│   ├── init-postgres.sql           # DDL for aggregated_metrics, risk_signals, dlq_events
│   └── local/                      # Shell scripts for local Kafka, Flink, and schema setup
│
└── docs/                           # Handover notes and architecture docs
```

---

## Pipeline DAG

The pipeline processes events through 10 sequential stages:

| Stage | Transform | Purpose |
|---|---|---|
| 1 | `ParseEventFn` | JSON → `BaseEvent` polymorphic deserialization; parse failures → DLQ |
| 2 | `ValidateEventFn` | Business rule checks (non-null IDs, non-negative amounts); failures → DLQ |
| 3 | `AssignEventTimestampFn` | Promotes `event_time` field to Beam event-time for watermarking |
| 4 | `DeduplicateEventsFn` | Stateful BagState deduplication with 24-hour event-time TTL expiry timer |
| 5 | `EnrichOrderFn` | Adds product category via side input catalog lookup map (defensive copy) |
| 6 | `RevenueAggregateCombineFn` | 5-minute tumbling window revenue + order count combiner; `StampWindowMetadataFn` injects `window_start`/`window_end` |
| 7 | `CustomerRiskDetectorFn` | Per-customer CombiningState fraud scorer (≥3 failures → +0.50; ≥10 velocity → +0.30; escalated at ≥0.80) |
| 8 | `CartAbandonmentFn` | Session window (30-min gap) cart abandonment detector |
| 9 | DLQ merge + sinks | Merged DLQ → **PostgreSQL + Kafka + Object Storage**; metrics + risk signals → PostgreSQL + Kafka + S3 |

---

## API & Swagger Documentation

The project includes an **OpenAPI 3.1.0 specification** and a **standalone interactive Swagger UI viewer** documenting all 10 domain event schemas, HTTP Ingestion endpoints, Analytics & Aggregated Metrics queries, Risk Signals API, Dead Letter Queue (DLQ) replay, and Traffic Simulator controls.

### Viewing the Interactive Swagger UI
Open [`docs/swagger-ui.html`](docs/swagger-ui.html) directly in any browser (e.g. Chrome, Firefox, Safari):

```bash
# macOS
open docs/swagger-ui.html
```

Or serve via any static web server:
```bash
npx serve docs/
# or
python3 -m http.server 8080 -d docs/
```

### OpenAPI Specification Files
- **OpenAPI 3.1.0 Specification (YAML)**: [`docs/openapi.yaml`](docs/openapi.yaml)
- **Interactive Swagger UI**: [`docs/swagger-ui.html`](docs/swagger-ui.html)

---

## Quick Start (Local)

### Prerequisites

- Java 17 (JDK), Maven 3.9+
- PostgreSQL (via Homebrew: `brew install postgresql@17`)
- No Docker required

### Step 1: Setup local binaries

```bash
make setup
# Downloads local Kafka and Flink binaries; sets script permissions
```

### Step 2: Start local services

```bash
# Terminal 1: Start Kafka (KRaft, no Zookeeper)
./scripts/local/start-kafka.sh

# Terminal 2: Create Kafka topics
./scripts/local/create-topics.sh

# Terminal 3: Start PostgreSQL and apply DDL schema
./scripts/local/init-schema.sh
```

### Step 3: Build the project

```bash
mvn clean install -DskipTests
# Produces:
#   pipeline/target/pipeline-1.0.0-SNAPSHOT.jar
#   simulator/target/simulator-1.0.0-SNAPSHOT.jar
```

### Step 4: Run the event simulator

```bash
# Send 10,000 events at 500 events/second
java -jar simulator/target/simulator-1.0.0-SNAPSHOT.jar \
  --rate 500 \
  --events 10000

# Available options: --rate, --events, --duration, --malformed-rate, --duplicate-rate, --seed
```

### Step 5: Run the pipeline (DirectRunner — no Flink cluster required)

```bash
java -jar pipeline/target/pipeline-1.0.0-SNAPSHOT.jar
```

### Step 6 (Optional): Run on local Flink standalone cluster

```bash
./scripts/local/start-flink.sh      # Starts Flink JobManager on :8081
# Then submit via Flink REST API or Flink CLI
```

---

## Running the Tests

```bash
# Unit + integration tests (all 41 test methods, no services required)
mvn test -pl platform-core,pipeline -am

# Expected: BUILD SUCCESS — Tests run: 41, Failures: 0, Errors: 0
```

Test classes — 41 test methods total:
- `PipelineConfigTest` — 10 tests: defaults, validation, Kafka timeouts, offset reset, checkpoint URI
- `IdempotencyKeyBuilderTest` — 5 tests: key construction
- `ParseEventFnTest` — 3 tests: JSON parsing, DLQ routing, empty payload silent skip
- `ValidateEventFnTest` — 6 tests: business rule validation, event_time range
- `DeduplicateEventsFnTest` — 4 tests: BagState deduplication TTL, 3-distinct-IDs pass-through
- `RevenueAggregateCombineFnTest` — 4 tests: revenue combiner, cancellations, avgOrderValue uses grossRevenue, two-pane windowing
- `CustomerRiskDetectorFnTest` — 3 tests: stateful fraud scoring, velocity + failure escalation, timer guard
- `CartAbandonmentFnTest` — 4 tests: session abandonment, completed-session no-signal, view-only no-signal
- `EnrichOrderFnTest` — 2 tests: deep-copy no-mutation, unknown product → "General" category
- `LateDataWindowingTest` — 3 tests: two-pane windowing, stale event DLQ, configurable max age
- `KafkaTimestampExtractorTest` — 6 tests: timestamp fallback hierarchy (JSON > Kafka ts > EPOCH)
- `PostgresSinkDoFnTest` — 3 tests: SQL pane_index guard, conflict target, column coverage
- `ObjectStorageSinkWrapperTest` — 1 test: windowed file write, no year 290308 in filenames
- `StreamingPipelineTest` — 2 tests: full DAG smoke test + DLQ routing `PAssert`

---

## Configuration Reference

All configuration is read from **environment variables** with sane local defaults. No config files to edit.

| Environment Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC_ORDERS` | `orders` | Input topic for order events |
| `KAFKA_TOPIC_PAYMENTS` | `payments` | Input topic for payment events |
| `KAFKA_TOPIC_DLQ` | `dlq` | Dead-letter output topic |
| `KAFKA_TOPIC_ALERTS` | `alerts` | Risk signal output topic |
| `KAFKA_TOPIC_METRICS` | `metrics-output` | Aggregated metrics output topic |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | Kafka consumer offset reset policy (`earliest`/`latest`) |
| `KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS` | `5000` (LOCAL) / `120000` (PROD) | Producer delivery timeout |
| `KAFKA_PRODUCER_REQUEST_TIMEOUT_MS` | `3000` (LOCAL) / `30000` (PROD) | Producer request timeout |
| `KAFKA_PRODUCER_MAX_BLOCK_MS` | `3000` (LOCAL) / `60000` (PROD) | Producer max block on send |
| `POSTGRES_URL` | `jdbc:postgresql://localhost:5432/ecommerce` | JDBC connection string |
| `POSTGRES_USER` | `postgres` | Database username |
| `POSTGRES_PASSWORD` | `postgres` | Database password |
| `POSTGRES_POOL_MIN` | `2` | HikariCP min idle connections |
| `POSTGRES_POOL_MAX` | `5` | HikariCP max pool size |
| `MAX_EVENT_AGE_DAYS` | `7` | Events older than this are routed to DLQ |
| `USE_LOCAL_OBJECT_STORAGE` | `true` | `false` → write to S3/MinIO |
| `LOCAL_OBJECT_STORAGE_PATH` | `./data/events` | Local output directory |
| `OBJECT_STORAGE_BUCKET` | `ecommerce-events` | S3/MinIO bucket name |
| `CHECKPOINT_DIR` | `./data/checkpoints` | Flink checkpoint dir (normalized to `file:///` URI) |
| `FLINK_PARALLELISM` | `2` | Default operator parallelism |
| `WATERMARK_INTERVAL_MS` | `1000` | Periodic watermark interval |
| `ALLOWED_LATENESS_SECONDS` | `600` | Late event acceptance window (10 min) |
| `DEDUPLICATION_TTL_SECONDS` | `86400` | Dedup state TTL (24 hours) |
| `ENVIRONMENT` | `LOCAL` | `LOCAL` or `PRODUCTION` |

---

## Key Design Decisions

### Dual-Target Strategy
All components run unchanged on both the 8 GB developer Mac (DirectRunner) and the enterprise Flink Kubernetes cluster (FlinkRunner). Runner selection is controlled by the `--runner` JVM argument. No code branches on environment.

### Dead-Letter Queue (DLQ)
Every stage that can fail (parse, validate) routes bad records to a typed `DlqEnvelope` with error category, code, stack trace, and Kafka offset metadata. DLQ records are written to PostgreSQL for operational replay and Kafka for downstream alerting.

### Stateful Deduplication
Uses `BagState<String>` keyed by `event_id` with a 24-hour event-time TTL timer. On laptop, state is backed by in-memory / RocksDB local. In production, Flink backs state with incremental RocksDB snapshots to S3.

### Windowing
- **Tumbling 5-min**: Revenue and order aggregation (combiner pattern, associative + commutative)
- **Sliding 5-min/1-min**: Available for trend analysis (not wired to main topology by default)
- **Session 30-min gap**: Cart abandonment detection; accumulating panes to capture full session

### Side Input Catalog
Product catalog is broadcast as a `PCollectionView<Map<String, String>>` using `View.asMap()`. In production, this is refreshed periodically via a bounded side input backed by a database snapshot. On laptop, it uses a hardcoded in-memory catalog.

---

## Technology Versions

See [`docs/technology-versions.md`](docs/technology-versions.md) for full rationale.

| Component | Version |
|---|---|
| Java | 17 (LTS) |
| Apache Beam | 2.75.0 |
| Apache Flink | 2.2.1 |
| Apache Kafka | 4.3.1 |
| PostgreSQL JDBC | 42.7.4 |
| HikariCP | 5.1.0 |
| Jackson | 2.18.2 |

---

## Enterprise Deployment

See [`docs/ENTERPRISE_DEPLOYMENT.md`](docs/ENTERPRISE_DEPLOYMENT.md) for a step-by-step guide to deploying this pipeline in a production Kubernetes environment with Flink Kubernetes Operator, S3 checkpointing, Kafka TLS, and Prometheus observability.

---

## Documentation

| Document | Purpose |
|---|---|
| [`docs/swagger-ui.html`](docs/swagger-ui.html) | Interactive Swagger UI API & Event Schema Catalog explorer |
| [`docs/openapi.yaml`](docs/openapi.yaml) | Full OpenAPI 3.1.0 specification for ingestion, metrics, risk, DLQ & simulator |
| [`docs/HANDOVER.md`](docs/HANDOVER.md) | Complete project handover — all phases, known limitations, key files |
| [`docs/DESIGN.md`](docs/DESIGN.md) | Full architecture reference, ADRs, open items |
| [`docs/ENTERPRISE_DEPLOYMENT.md`](docs/ENTERPRISE_DEPLOYMENT.md) | Flink/Kubernetes production deployment guide |
