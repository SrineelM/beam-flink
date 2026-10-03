# Architecture Design Document
# Enterprise Real-Time E-Commerce Order Intelligence Platform

**Version:** 1.0  
**Stack:** Apache Beam 2.75.0 · Apache Flink 2.2.1 · Apache Kafka 4.3.1 · Java 17  
**Status:** Production-Ready Reference Implementation

---

## 1. Problem Statement

An e-commerce platform generates tens of thousands of heterogeneous domain events per second across order, payment, cart, and shipment lifecycles. The platform needs to:

1. **Ingest** events reliably from Kafka with schema-resilient parsing.
2. **Validate & deduplicate** events within tight latency budgets (sub-second end-to-end).
3. **Aggregate revenue metrics** in real-time over multiple windowing strategies.
4. **Detect fraud and risk** per customer using stateful, long-horizon accumulators.
5. **Detect cart abandonment** via session analysis.
6. **Persist results** to analytical stores (PostgreSQL), streaming buses (Kafka), and cold storage (S3/object storage).
7. **Operate on a developer laptop** (8 GB RAM, no Docker) without code changes and **scale to 50,000 EPS** on Kubernetes.

---

## 2. System Architecture

### 2.1 High-Level Component Diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Apache Flink Cluster (Production)                    │
│                     / DirectRunner (Local Development)                       │
│                                                                              │
│  ┌─────────────┐    ┌──────────────────────────────────────────────────────┐│
│  │   Kafka      │    │                 Apache Beam Pipeline                 ││
│  │  (Source)    │    │                                                      ││
│  │  orders      │──► │  ParseEventFn → ValidateEventFn → Dedup → Enrich    ││
│  │  payments    │    │       ↓(DLQ)        ↓(DLQ)                          ││
│  │  customer-   │    │  AssignTimestamp → [TumblingWindow + SessionWindow]  ││
│  │  events      │    │                         ↓                           ││
│  └─────────────┘    │  RevenueAggregateCombineFn + CustomerRiskDetectorFn  ││
│                      │  + CartAbandonmentFn                                 ││
│                      └──────────────────────────────────────────────────────┘│
│                                         ↓                                    │
│             ┌──────────────────────────────────────────────────┐             │
│             │                      Sinks                        │             │
│             │  PostgreSQL (JDBC/HikariCP) │ Kafka │ S3/MinIO   │             │
│             └──────────────────────────────────────────────────┘             │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Module Responsibilities

| Module | Responsibility | Key Class |
|---|---|---|
| `platform-core` | Shared domain models, config, utilities | `BaseEvent`, `PipelineConfig` |
| `pipeline` | Beam DAG assembly, all transforms, sinks | `StreamingPipeline` |
| `simulator` | Synthetic load generator with fault injection | `Simulator`, `EventFactory` |

---

## 3. Domain Model

### 3.1 Event Type Hierarchy

`BaseEvent` is an abstract Jackson-polymorphic base class. `event_type` field drives deserialization to a concrete subclass via `@JsonSubTypes`.

```
BaseEvent (abstract)
├── OrderCreatedEvent         – event_type: ORDER_CREATED
├── OrderUpdatedEvent         – event_type: ORDER_UPDATED
├── OrderCancelledEvent       – event_type: ORDER_CANCELLED
├── PaymentAuthorizedEvent    – event_type: PAYMENT_AUTHORIZED
├── PaymentFailedEvent        – event_type: PAYMENT_FAILED
├── ShipmentCreatedEvent      – event_type: SHIPMENT_CREATED
├── ShipmentDeliveredEvent    – event_type: SHIPMENT_DELIVERED
├── CustomerViewedProductEvent – event_type: CUSTOMER_VIEWED_PRODUCT
├── ProductAddedToCartEvent   – event_type: PRODUCT_ADDED_TO_CART
└── ProductRemovedFromCartEvent – event_type: PRODUCT_REMOVED_FROM_CART
```

**Common fields on `BaseEvent`:**
- `event_id` (UUID) — globally unique, used as deduplication key
- `event_type` (enum) — drives polymorphic deserialization
- `event_time` (Instant ISO-8601) — business event timestamp, used for watermarking
- `customer_id` (String) — used as per-customer state key
- `amount` (BigDecimal) — financial value; validated non-negative
- `correlation_id` (UUID) — cross-service trace correlation
- `metadata` (Map) — extensible key-value bag for category enrichment

### 3.2 Output Models

| Model | Written To | Purpose |
|---|---|---|
| `AggregatedMetric` | PostgreSQL `aggregated_metrics`, Kafka `metrics-output` | Windowed revenue/order KPIs |
| `RiskSignal` | PostgreSQL `risk_signals`, Kafka `alerts` | Per-customer fraud score |
| `DlqEnvelope` | PostgreSQL `dlq_events`, Kafka `dlq` | Bad event audit trail |

---

## 4. Pipeline Transform Design

### 4.1 ParseEventFn

- **Input:** `PCollection<String>` (raw Kafka JSON payloads)
- **Output:** `PCollectionTuple` — valid `BaseEvent` | `DlqEnvelope`
- **Pattern:** Multi-output `TupleTag` fan-out
- **Error handling:** Any `JsonProcessingException` is caught, wrapped in `DlqEnvelope` with `ErrorCategory.PARSE_ERROR`, and emitted to `DLQ_TAG`. Stack trace is captured for debugging.
- **Why not Avro schema registry here?** Avro requires a schema registry service. For cross-team interoperability and simpler onboarding, JSON with Jackson polymorphism is used. Avro migration path is documented in Phase N+1.

### 4.2 ValidateEventFn

- **Input:** `PCollection<BaseEvent>` (parsed, not yet validated)
- **Validation rules:**
  1. `event_id` must not be null (uniqueness prerequisite)
  2. `customer_id` must be non-null and non-empty (attribution requirement)
  3. `amount`, if present, must be ≥ 0 (financial integrity)
- **Output:** valid events pass through; invalid → `DlqEnvelope` with `ErrorCategory.SCHEMA_VALIDATION_ERROR` or `BUSINESS_RULE_VIOLATION`
- **Why not Bean Validation (JSR-380)?** Beam DoFns serialize to worker JVMs; annotation-based validators require reflection context. Explicit imperative checks are simpler and serialization-safe.

### 4.3 AssignEventTimestampFn

- **Purpose:** Bridges business event time (`event_time` field) to Beam's internal watermark system.
- **Why this matters:** Without this, Beam uses processing time for windows, defeating the point of tumbling/session windows for historical replay or late events.
- **Fallback:** Events without `event_time` are emitted with processing timestamp (current Beam element timestamp), not dropped.

### 4.4 DeduplicateEventsFn

- **State:** `BagState<String>` keyed by `event_id`
- **TTL:** 24-hour event-time timer clears state
- **Why BagState over ValueState<Set>?** `BagState` is append-only and more efficient for incremental writes. The linear scan for membership check is acceptable at typical deduplication cardinality (< 10,000 unique IDs per key partition per day).
- **Production upgrade:** For higher cardinality, replace with Bloom filter in `ValueState<BloomFilter>` to reduce memory footprint from O(n) to O(1) per lookup.

### 4.5 EnrichOrderFn

- **Pattern:** Beam side input (`PCollectionView<Map<String, String>>`)
- **Catalog:** Product ID → Category mapping broadcast to all workers
- **Production pattern:** Refresh the side input bounded PCollection from a database snapshot every N minutes using a recurring bounded read. The current implementation uses a static in-memory catalog seeded in `StreamingPipeline`.
- **Enrichment:** Writes `category` into the event's `metadata` map (non-destructive, additive enrichment).

### 4.6 RevenueAggregateCombineFn

- **Window:** 5-minute tumbling (`FixedWindows.of(Duration.standardMinutes(5))`)
- **Pattern:** `Combine.globally()` with custom `CombineFn` — associative + commutative, enabling partial aggregation at each worker before merging.
- **Accumulator fields:** `totalRevenue`, `orderCount`, `failedPaymentCount`, `cancellationCount`, `uniqueCustomers` (Set — note: sets are not mergeable across workers without full serialization; use HyperLogLog for production cardinality at scale).
- **Output:** `AggregatedMetric` with derived `avgOrderValue` and `failedPaymentRate`.

### 4.7 CustomerRiskDetectorFn

- **State:**
  - `paymentFailures`: `CombiningState<Integer, int[], Integer>` — running sum of `PAYMENT_FAILED` events
  - `orderAttempts`: `CombiningState<Integer, int[], Integer>` — running sum of `ORDER_CREATED` events
- **Timer:** 1-hour event-time `sessionExpiry` timer resets both states on inactivity
- **Risk scoring logic:**
  - `failures ≥ 3` → +0.50 risk score (strong fraud signal)
  - `attempts ≥ 10` → +0.30 risk score (velocity signal)
  - `score ≥ 0.80` → escalated flag set (triggers alert routing)
  - Only emits a `RiskSignal` when `score ≥ 0.50` (avoids noisy low-risk signals)

### 4.8 CartAbandonmentFn

- **Window:** 30-minute session gap (`Sessions.withGapDuration(30min)`)
- **Input:** `Iterable<BaseEvent>` per session (from `GroupByKey` after session windowing)
- **Logic:** If session contains `PRODUCT_ADDED_TO_CART` but no `ORDER_CREATED` → cart abandonment `RiskSignal` with `riskScore=0.2`
- **Pane mode:** `accumulatingFiredPanes()` ensures full session state is available when the window fires (important for session completeness).

---

## 5. Sink Design

### 5.1 PostgresSinkDoFn

- **Connection pool:** HikariCP with min/max pool size from `PipelineConfig`
- **Type dispatch:** Single generic `DoFn<T, Void>` uses `instanceof` branching to route to the correct `INSERT` statement for `AggregatedMetric`, `RiskSignal`, or `DlqEnvelope`.
- **Resilience:** Connection pool initialized lazily in `@Setup`; re-initialized if null in `processElement` (handles worker restart). Failures logged but not re-thrown (records are effectively dropped, logged for DLQ-layer monitoring).
- **Production upgrade:** Wrap `executeUpdate()` in `RetryUtil.executeWithRetry()` for transient JDBC failures.

### 5.2 KafkaSinkDoFn

- **Producer:** Native `KafkaProducer<String, String>` — fire-and-forget mode (`acks=1`)
- **Durability:** For at-least-once guarantees, increase to `acks=all` and enable idempotent producer.
- **Usage in topology:** Alerts topic for `RiskSignal`, metrics topic for `AggregatedMetric`, DLQ topic for `DlqEnvelope`.

### 5.3 ObjectStorageSinkWrapper

- **Implementation:** Wraps `TextIO.write()` with windowed writes and 1 shard per window.
- **Dual path:** `USE_LOCAL_OBJECT_STORAGE=true` → local filesystem; `false` → S3-compatible endpoint.
- **File naming:** `{prefix}-{window-start}-{shard}.{suffix}` (Beam default pattern).

---

## 6. Windowing Strategy

| Strategy | Class Method | Window Size | Slide | Use Case |
|---|---|---|---|---|
| Tumbling 1-min | `tumbling1Min()` | 1 minute | — | Short-burst metrics |
| Tumbling 5-min | `tumbling5Min()` | 5 minutes | — | Revenue aggregation (default) |
| Sliding 5-min/1-min | `sliding5Min1MinSlide()` | 5 minutes | 1 minute | Trend analysis |
| Session 30-min gap | `session30MinGap()` | Variable | — | Cart abandonment |

All strategies use `AfterWatermark.pastEndOfWindow()` triggers and allow 10 minutes of late data.

---

## 7. Configuration Architecture

`PipelineConfig` reads all settings from environment variables with local-first defaults. It is `Serializable` and passed by value to every `DoFn` constructor — Beam serializes it to each worker JVM.

**Design constraint:** `PipelineConfig` must not hold non-serializable fields. JDBC `DataSource` and `KafkaProducer` are `transient` and re-initialized lazily per-worker.

---

## 8. Error Handling & Resilience Strategy

| Layer | Failure Type | Response |
|---|---|---|
| Parse | Invalid JSON | DLQ envelope → PostgreSQL + Kafka `dlq` topic |
| Validation | Business rule violation | DLQ envelope → PostgreSQL + Kafka `dlq` topic |
| Deduplication | Duplicate `event_id` | Silently dropped (no DLQ — expected behavior) |
| JDBC sink | DB connection failure | Log + skip (record lost); operator-level retry via Flink |
| Kafka sink | Broker unavailable | Log + skip (record lost); Kafka producer internal retry=3 |
| State | Worker failure/restart | Flink checkpoints restore `BagState` and `CombiningState` from last checkpoint |

---

## 9. Observability

| Signal | Implementation | Local | Production |
|---|---|---|---|
| Structured logs | Logback + logstash-logback-encoder (JSON format) | Console stdout | ELK / Splunk shipper |
| MDC context | `StructuredLogger` wraps SLF4J MDC | Per-log event_id + correlation_id | Same |
| Metrics | Beam built-in counters (accessible via Flink REST) | Flink Web UI :8081 | Prometheus scrape |
| Tracing | OpenTelemetry SDK (1.44.1) | Console OTLP export | Jaeger / Tempo |
| Checkpoints | Flink checkpoint API | `./data/checkpoints` on local disk | S3 incremental RocksDB snapshots |

---

## 10. Data Flow & Topic Schema

### Kafka Topics

| Topic | Direction | Key | Value | Partitions (Prod) |
|---|---|---|---|---|
| `orders` | Input | `order_id` | `BaseEvent` JSON | 12 |
| `payments` | Input | `order_id` | `BaseEvent` JSON | 12 |
| `customer-events` | Input | `customer_id` | `BaseEvent` JSON | 6 |
| `dlq` | Output | `correlation_id` | `DlqEnvelope` JSON | 3 |
| `alerts` | Output | `customer_id` | `RiskSignal` JSON | 6 |
| `metrics-output` | Output | `window_start` | `AggregatedMetric` JSON | 3 |

### PostgreSQL Tables

| Table | Populated By | Indexes |
|---|---|---|
| `aggregated_metrics` | `RevenueAggregateCombineFn` → `PostgresSinkDoFn` | `(window_start, window_end)` |
| `risk_signals` | `CustomerRiskDetectorFn`, `CartAbandonmentFn` → `PostgresSinkDoFn` | `(customer_id)` |
| `dlq_events` | `ParseEventFn`, `ValidateEventFn` → `PostgresSinkDoFn` | `(error_category)` |

---

## 11. Testing Strategy

**Total: 41 tests, 0 failures** (as of 2026-10-03). All tests use `Pipeline.create()` (DirectRunner) — no services required. Beam's `PAssert` validates output `PCollection` contents.

| Test Class | Type | What It Tests |
|---|---|---|
| `PipelineConfigTest` | Unit | Defaults, validation, Kafka timeouts, offset reset, checkpoint URI, maxEventAgeDays |
| `IdempotencyKeyBuilderTest` | Unit | Idempotency key construction |
| `ParseEventFnTest` | Unit (TestPipeline) | Valid parse, malformed JSON → DLQ, empty payload silent skip |
| `ValidateEventFnTest` | Unit (TestPipeline) | All 6 validation rules including event_time range |
| `DeduplicateEventsFnTest` | Unit (TestPipeline) | Duplicate suppression, distinct IDs all pass |
| `RevenueAggregateCombineFnTest` | Unit (TestPipeline) | Combiner math, avgOrderValue uses grossRevenue (T1.4), two-pane windowing |
| `CustomerRiskDetectorFnTest` | Unit (TestPipeline) | Stateful risk scoring, timer guard (T1.5) |
| `CartAbandonmentFnTest` | Unit (TestPipeline) | Session abandonment, no signal on completion |
| `EnrichOrderFnTest` | Unit (TestPipeline) | Deep-copy no-mutation (T1.6), unknown product category |
| `LateDataWindowingTest` | Unit (TestPipeline) | Two-pane windowing (T1.1), stale event DLQ (T1.9), configurable maxEventAgeDays |
| `KafkaTimestampExtractorTest` | Unit | Timestamp fallback: JSON → Kafka ts → EPOCH (T2.3) |
| `PostgresSinkDoFnTest` | Unit | SQL pane_index guard (T1.3), conflict target, column coverage |
| `ObjectStorageSinkWrapperTest` | Integration (TestPipeline) | Windowed file write; no year 290308 in filenames (T2.4) |
| `StreamingPipelineTest` | Integration (TestPipeline) | Full E2E: DLQ, dedup, metrics, fraud signal, cart abandonment (T1.1) |

---

## 12. Known Behaviours

### 12.1 Kafka Timestamp Extraction Fallback (T2.3)

`KafkaTimestampExtractor` uses a three-tier hierarchy to assign event-time to Kafka records:

1. **`event_time` field in JSON payload** (preferred — highest precision)
2. **Kafka record timestamp** (`kafkaTs > 0`) — broker or producer-assigned
3. **Joda EPOCH (0 ms)** — deterministic last resort; records with epoch timestamp will become very-late and `ValidateEventFn` routes them to DLQ via `ERR_EVENT_TIME_OUT_OF_RANGE`.

**Never use `Instant.now()` as fallback** — it makes the watermark non-reproducible and prevents exactly-once replay.

### 12.2 DirectRunner Multi-Stage Watermark Propagation

In a multi-stage pipeline (e.g. `StreamingPipelineTest` with Parse → Validate → Dedup → Enrich → Window), the watermark propagates lazily through each DoFn stage. This means:

- An element added after `advanceWatermarkTo(windowEnd+2min)` may reach the windowing stage **before** the watermark catches up.
- On-time and late elements may be consolidated into a single pane in DirectRunner E2E tests.
- **This is a test harness limitation, not a Flink production issue.** Flink processes each stage's watermark synchronously.
- Workaround: use a 2-stage pipeline (TestStream → Window) to verify pane semantics. See `LateDataWindowingTest.testFiveMinWindowProducesTwoPanes`.

### 12.3 Checkpoint URI Requirements (T1.10)

Flink's checkpoint storage requires a URI scheme. A bare relative path like `./data/checkpoints` causes a runtime `IllegalArgumentException`. `PipelineConfig.getCheckpointDirUri()` normalizes and wraps the path as a `file:///`-prefixed absolute URI. Cloud paths (e.g. `s3://`, `gs://`) are passed through unchanged.

---

## 13. Simulator Design

The simulator generates synthetic events matching the `BaseEvent` schema:

- **EventFactory:** Creates all 10 event types with realistic fields (UUID IDs, BigDecimal amounts, ISO timestamps).
- **OrderStateMachine:** Tracks per-order state (PENDING → PAYMENT_AUTHORIZED → SHIPPED → DELIVERED) to generate realistic event sequences.
- **Fault injection:** Configurable `--malformed-rate` (bad JSON) and `--duplicate-rate` (re-send same event) to test DLQ and dedup paths.
- **Rate control:** Events per second controlled by `Thread.sleep(1000)` per batch — suitable for laptop; production load testing uses JMH or Gatling.

---

## 14. Open Items & Planned Enhancements

| Item | Priority | Notes |
|---|---|---|
| Avro schema registry integration | High | Replace JSON with Avro for schema evolution support |
| HyperLogLog for `uniqueCustomers` | Medium | Replace `HashSet` in combiner — doesn't scale across workers |
| Side input periodic refresh | Medium | Database-backed catalog refresh every 5 minutes |
| DLQ replay mechanism | Medium | Ability to re-process DLQ events after fixing root cause |
| Schema compatibility tests | Low | Avro schema evolution checks in CI |
