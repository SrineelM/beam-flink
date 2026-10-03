# Enterprise Production Deployment Guide
# Apache Beam + Flink Streaming Platform on Kubernetes

**Audience:** Platform / Data Engineering teams  
**Stack:** Apache Beam 2.75.0 · Apache Flink 2.2.1 · Kubernetes 1.29+  
**Pattern:** Flink Kubernetes Operator 1.9.0 · FlinkDeployment CRD

---

## Overview

This guide covers the complete production deployment of the Enterprise Real-Time E-Commerce Order Intelligence Platform in an enterprise Kubernetes environment. It addresses:

1. Infrastructure prerequisites
2. Container image build strategy
3. Flink cluster provisioning via Kubernetes Operator
4. Kafka cluster configuration for 50,000 EPS
5. PostgreSQL and object storage setup
6. Pipeline job submission and lifecycle
7. Security hardening (TLS, secrets management)
8. Observability (Prometheus, Grafana, OpenTelemetry)
9. Operational runbook (scaling, checkpointing, upgrades)
10. Disaster recovery and DLQ replay

---

## 1. Infrastructure Prerequisites

### 1.1 Kubernetes Cluster Requirements

| Component | Minimum (Prod) | Recommended |
|---|---|---|
| Kubernetes version | 1.28 | 1.29+ |
| Node count | 6 nodes | 10+ nodes |
| Node size (Flink workers) | 8 vCPU / 32 GB RAM | 16 vCPU / 64 GB RAM |
| Node size (Kafka) | 4 vCPU / 16 GB RAM | 8 vCPU / 32 GB RAM |
| Storage class | SSD-backed (GP3) | NVMe local storage for RocksDB |
| Networking | Flannel / Calico CNI | Calico with NetworkPolicy |

### 1.2 Required Cluster Add-ons

```bash
# 1. Cert-Manager (for Flink Operator webhook TLS)
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.0/cert-manager.yaml

# 2. Flink Kubernetes Operator
helm repo add flink-operator https://downloads.apache.org/flink/flink-kubernetes-operator-1.9.0/
helm install flink-kubernetes-operator flink-operator/flink-kubernetes-operator \
  --namespace flink-operator \
  --create-namespace \
  --set webhook.create=true

# 3. Prometheus Operator (for metrics scraping)
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace monitoring --create-namespace

# 4. Strimzi Kafka Operator (optional — use if self-managing Kafka)
helm repo add strimzi https://strimzi.io/charts/
helm install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
  --namespace kafka --create-namespace
```

---

## 2. Container Image Build

### 2.1 Multi-Stage Dockerfile

Create `infrastructure/docker/Dockerfile.pipeline`:

```dockerfile
# Stage 1: Build — uses Maven to compile and shade the fat JAR
FROM eclipse-temurin:17-jdk-jammy AS builder
WORKDIR /build
COPY pom.xml .
COPY platform-core/ platform-core/
COPY pipeline/ pipeline/
COPY simulator/ simulator/
# Download dependencies first (Docker layer caching)
RUN mvn dependency:go-offline -q
# Build only the pipeline module fat JAR (skips tests; CI runs tests separately)
RUN mvn clean install -pl platform-core,pipeline -am -DskipTests -q

# Stage 2: Runtime — minimal JRE image
FROM eclipse-temurin:17-jre-jammy
LABEL maintainer="platform-data-engineering@enterprise.com"
LABEL version="1.0.0"

# Create non-root user (security requirement)
RUN groupadd -r beam && useradd -r -g beam beam

# Copy only the shaded fat JAR — no Maven, no build tools
COPY --from=builder /build/pipeline/target/pipeline-1.0.0-SNAPSHOT.jar /app/pipeline.jar

# Flink expects the job JAR in a standard location for FlinkDeployment CRD
RUN mkdir -p /opt/flink/usrlib && cp /app/pipeline.jar /opt/flink/usrlib/

USER beam
ENTRYPOINT ["java", "-jar", "/app/pipeline.jar"]
```

### 2.2 Build and Push to Registry

```bash
# Set your registry (ECR, GCR, Harbor, etc.)
REGISTRY="your-registry.example.com/platform"
TAG="1.0.0-$(git rev-parse --short HEAD)"

docker build -f infrastructure/docker/Dockerfile.pipeline \
  -t "${REGISTRY}/beam-pipeline:${TAG}" \
  -t "${REGISTRY}/beam-pipeline:latest" \
  .

docker push "${REGISTRY}/beam-pipeline:${TAG}"
docker push "${REGISTRY}/beam-pipeline:latest"
```

---

## 3. Secrets Management

### 3.1 Kubernetes Secrets

Never store credentials in ConfigMaps or environment variable literals in deployment manifests. Use Kubernetes Secrets (ideally backed by a secrets manager like AWS Secrets Manager, HashiCorp Vault, or GCP Secret Manager).

```bash
# Create secrets namespace
kubectl create namespace beam-platform

# PostgreSQL credentials
kubectl create secret generic postgres-credentials \
  --namespace beam-platform \
  --from-literal=POSTGRES_URL="jdbc:postgresql://postgres-primary.db.svc.cluster.local:5432/ecommerce" \
  --from-literal=POSTGRES_USER="beam_writer" \
  --from-literal=POSTGRES_PASSWORD="$(openssl rand -base64 32)"

# Kafka credentials (SASL/SCRAM or mTLS — use your auth mechanism)
kubectl create secret generic kafka-credentials \
  --namespace beam-platform \
  --from-literal=KAFKA_BOOTSTRAP_SERVERS="kafka-brokers.kafka.svc.cluster.local:9092"

# S3 / object storage credentials
kubectl create secret generic s3-credentials \
  --namespace beam-platform \
  --from-literal=AWS_ACCESS_KEY_ID="..." \
  --from-literal=AWS_SECRET_ACCESS_KEY="..." \
  --from-literal=OBJECT_STORAGE_BUCKET="ecommerce-events-prod"
```

### 3.2 Vault Integration (recommended)

For teams using HashiCorp Vault, use the Vault Agent Injector or the Vault Secrets Operator to inject secrets as files or environment variables at pod startup. This avoids secrets stored in etcd.

---

## 4. Flink Cluster Provisioning

### 4.1 FlinkDeployment CRD (Application Mode)

Save as `infrastructure/kubernetes/flink-deployment.yaml`:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: beam-ecommerce-pipeline
  namespace: beam-platform
spec:
  image: your-registry.example.com/platform/beam-pipeline:1.0.0-abcdef
  flinkVersion: v2_2

  # Flink configuration overrides
  flinkConfiguration:
    # Checkpointing: every 60 seconds to S3
    execution.checkpointing.interval: "60000"
    execution.checkpointing.mode: EXACTLY_ONCE
    execution.checkpointing.timeout: "300000"
    state.backend: rocksdb
    state.backend.incremental: "true"
    state.checkpoints.dir: s3://your-flink-checkpoints/beam-pipeline/checkpoints
    state.savepoints.dir: s3://your-flink-checkpoints/beam-pipeline/savepoints
    # RocksDB memory tuning for stateful deduplication and risk detection
    state.backend.rocksdb.memory.managed: "true"
    state.backend.rocksdb.memory.write-buffer-size: 64mb
    state.backend.rocksdb.memory.high-prio-pool-ratio: "0.1"
    # Kafka source watermark alignment
    pipeline.auto-watermark-interval: "1000"
    # Parallelism
    parallelism.default: "12"
    # Prometheus metrics reporter
    metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
    metrics.reporter.prom.port: "9249"

  serviceAccount: beam-platform-sa

  jobManager:
    resource:
      memory: "2048m"
      cpu: 1.0
    replicas: 1

  taskManager:
    resource:
      memory: "8192m"    # Tune based on RocksDB state size
      cpu: 4.0
    replicas: 3           # Scale based on Kafka partition count (match or less)

  job:
    jarURI: local:///opt/flink/usrlib/pipeline.jar
    entryClass: com.enterprise.beam.pipeline.StreamingPipeline
    # Beam FlinkRunner args: --runner=FlinkRunner is the key switch
    args:
      - "--runner=FlinkRunner"
      - "--flinkMaster=:8081"
    upgradeMode: savepoint   # Use savepoints for zero-downtime upgrades

  # Mount Kubernetes secrets as environment variables
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          envFrom:
            - secretRef:
                name: postgres-credentials
            - secretRef:
                name: kafka-credentials
            - secretRef:
                name: s3-credentials
          env:
            - name: ENVIRONMENT
              value: "PRODUCTION"
            - name: FLINK_PARALLELISM
              value: "12"
            - name: POSTGRES_POOL_MIN
              value: "5"
            - name: POSTGRES_POOL_MAX
              value: "20"
            - name: USE_LOCAL_OBJECT_STORAGE
              value: "false"
            - name: WATERMARK_INTERVAL_MS
              value: "1000"
            - name: ALLOWED_LATENESS_SECONDS
              value: "600"
            - name: DEDUPLICATION_TTL_SECONDS
              value: "86400"
          resources:
            requests:
              memory: "8Gi"
              cpu: "4"
            limits:
              memory: "10Gi"
              cpu: "6"
```

Apply the deployment:

```bash
kubectl apply -f infrastructure/kubernetes/flink-deployment.yaml
```

---

## 5. Kafka Configuration for 50,000 EPS

### 5.1 Topic Configuration

```bash
# Use Kafka CLI or your Kafka admin client
KAFKA_BOOTSTRAP="kafka-brokers.kafka.svc.cluster.local:9092"

# Input topics: 12 partitions each (matches Flink parallelism)
kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} \
  --create --topic orders \
  --partitions 12 --replication-factor 3 \
  --config retention.ms=604800000 \
  --config segment.bytes=134217728

kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} \
  --create --topic payments \
  --partitions 12 --replication-factor 3

kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} \
  --create --topic customer-events \
  --partitions 6 --replication-factor 3

# Output topics
kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} \
  --create --topic dlq \
  --partitions 3 --replication-factor 3 \
  --config retention.ms=2592000000  # 30 days for DLQ replay

kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} \
  --create --topic alerts \
  --partitions 6 --replication-factor 3

kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} \
  --create --topic metrics-output \
  --partitions 3 --replication-factor 3
```

### 5.2 Broker Configuration

Key broker settings for 50,000 EPS at 50 KB average message size:

```properties
# Throughput tuning
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# Replication
default.replication.factor=3
min.insync.replicas=2

# Log compaction (for DLQ replay keyed by correlation_id)
log.cleanup.policy=delete
log.retention.hours=168
```

---

## 6. PostgreSQL Production Setup

### 6.1 Schema Migration

Use Flyway or Liquibase for managed schema migrations. Never apply `init-postgres.sql` directly in production — it's for local development only.

```bash
# Flyway example
flyway -url=jdbc:postgresql://postgres-primary:5432/ecommerce \
       -user=beam_admin \
       -password=... \
       -locations=filesystem:scripts/migrations \
       migrate
```

### 6.2 Connection Pool Tuning

For 12 Flink task slots, each with 1 `PostgresSinkDoFn` instance:

```
POSTGRES_POOL_MIN=5    # 5 idle connections per task slot
POSTGRES_POOL_MAX=20   # 20 max connections per task slot
# Total max connections: 12 task slots × 20 = 240 connections
# Ensure PostgreSQL max_connections >= 300 (includes headroom)
```

### 6.3 Read Replica for Analytics

Point BI tools (Superset, Metabase, Redash) to a read replica, **not** the primary. The pipeline writes to the primary; analytical queries on read replica avoid write contention.

---

## 7. S3 / Object Storage Setup

### 7.1 S3 Bucket Configuration

```bash
# Create bucket with versioning and lifecycle rules
aws s3api create-bucket \
  --bucket ecommerce-events-prod \
  --region us-east-1

# Enable versioning (protects against accidental deletes)
aws s3api put-bucket-versioning \
  --bucket ecommerce-events-prod \
  --versioning-configuration Status=Enabled

# Lifecycle: move data to Glacier after 90 days, delete after 2 years
aws s3api put-bucket-lifecycle-configuration \
  --bucket ecommerce-events-prod \
  --lifecycle-configuration file://infrastructure/s3-lifecycle.json
```

### 7.2 Flink Checkpoint Bucket

```bash
aws s3api create-bucket --bucket your-flink-checkpoints --region us-east-1

# Bucket policy: only allow Flink task manager IAM role
aws s3api put-bucket-policy \
  --bucket your-flink-checkpoints \
  --policy file://infrastructure/flink-checkpoint-bucket-policy.json
```

---

## 8. Security Hardening

### 8.1 Kafka TLS + SASL

```properties
# Kafka producer properties (KafkaSinkDoFn — update PipelineConfig to support these)
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
ssl.truststore.location=/etc/kafka/certs/kafka.truststore.jks
ssl.truststore.password=${KAFKA_TRUSTSTORE_PASSWORD}
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="${KAFKA_USERNAME}" password="${KAFKA_PASSWORD}";
```

### 8.2 PostgreSQL SSL

```
POSTGRES_URL=jdbc:postgresql://postgres-primary:5432/ecommerce?ssl=true&sslmode=verify-full&sslrootcert=/etc/postgres/certs/ca.crt
```

### 8.3 Kubernetes RBAC

```yaml
# ServiceAccount for the Flink pods
apiVersion: v1
kind: ServiceAccount
metadata:
  name: beam-platform-sa
  namespace: beam-platform
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: beam-platform-role
  namespace: beam-platform
rules:
  # Flink Operator needs to manage pods, services, configmaps
  - apiGroups: [""]
    resources: ["pods", "services", "configmaps"]
    verbs: ["get", "list", "watch", "create", "update", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: beam-platform-rolebinding
  namespace: beam-platform
roleRef:
  kind: Role
  name: beam-platform-role
subjects:
  - kind: ServiceAccount
    name: beam-platform-sa
```

---

## 9. Observability

### 9.1 Prometheus Metrics

Flink exposes metrics on port `9249` via the `PrometheusReporterFactory` configured in the `FlinkDeployment` above. Key metrics to alert on:

| Metric | Alert Threshold | Meaning |
|---|---|---|
| `flink_taskmanager_job_task_numRecordsInPerSecond` | < 1000/sec (unexpectedly low) | Input throughput drop |
| `flink_jobmanager_job_uptime` | Gap > 5 min | Job restarted |
| `flink_taskmanager_Status_JVM_Memory_Heap_Used` | > 85% | Memory pressure |
| `flink_taskmanager_job_task_currentInputWatermark` | Stuck (no progress) | Watermark stall |
| `flink_taskmanager_job_task_operator_numLateRecordsDropped` | > 0 | Late events being dropped |

### 9.2 Grafana Dashboard

Import the Flink community dashboard ID `14492` into Grafana. Add custom panels for:
- DLQ event rate (query PostgreSQL `dlq_events` count per minute)
- Risk signal escalation rate (query `risk_signals WHERE escalated=true`)
- Revenue aggregation lag (compare `aggregated_metrics.window_end` to `NOW()`)

### 9.3 OpenTelemetry Tracing

The `pom.xml` includes OpenTelemetry SDK 1.44.1. To enable trace export to Jaeger/Tempo in production:

```bash
# Add to FlinkDeployment job args:
- "--OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger-collector.monitoring.svc.cluster.local:4317"
- "--OTEL_SERVICE_NAME=beam-ecommerce-pipeline"
```

### 9.4 Structured Logging

All pipeline components use `StructuredLogger` which outputs JSON via logstash-logback-encoder. Ship logs to your ELK stack or Splunk via Fluentd/Fluent Bit DaemonSet.

Key log fields to index:
- `component` — which DoFn emitted the log
- `event_id` — trace individual events
- `correlation_id` — trace cross-service flows
- `error_code` — aggregate error types in dashboards

---

## 10. Pipeline Lifecycle Operations

### 10.1 Zero-Downtime Job Upgrade

The `FlinkDeployment` uses `upgradeMode: savepoint`. To upgrade:

1. Build and push new container image.
2. Update `spec.image` in `flink-deployment.yaml`.
3. Apply: `kubectl apply -f infrastructure/kubernetes/flink-deployment.yaml`
4. The Flink Kubernetes Operator will:
   a. Trigger a savepoint of the current job.
   b. Stop the current job.
   c. Start the new job from the savepoint.
5. Verify: `kubectl get flinkdeployment beam-ecommerce-pipeline -n beam-platform`

### 10.2 Manual Savepoint

```bash
# Trigger savepoint via Flink REST API
JM_POD=$(kubectl get pods -n beam-platform -l component=jobmanager -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n beam-platform ${JM_POD} -- \
  curl -s -X POST http://localhost:8081/jobs/<JOB_ID>/savepoints \
  -H "Content-Type: application/json" \
  -d '{"target-directory": "s3://your-flink-checkpoints/savepoints/"}'
```

### 10.3 Restart from Savepoint

```bash
# Update FlinkDeployment spec to point to savepoint:
spec:
  job:
    initialSavepointPath: "s3://your-flink-checkpoints/savepoints/<savepoint-id>"
```

### 10.4 Scaling Task Managers

```bash
# Scale by updating FlinkDeployment taskManager.replicas
kubectl patch flinkdeployment beam-ecommerce-pipeline -n beam-platform \
  --type=merge \
  --patch='{"spec":{"taskManager":{"replicas":6}}}'
```

---

## 11. DLQ Replay

Dead-letter events in the `dlq_events` PostgreSQL table and `dlq` Kafka topic can be replayed after fixing the root cause:

### 11.1 PostgreSQL-Based Replay

```sql
-- Identify DLQ events by category and time range
SELECT original_payload, error_code, pipeline_stage, processing_timestamp
FROM dlq_events
WHERE error_category = 'PARSE_ERROR'
  AND processing_timestamp > NOW() - INTERVAL '1 hour'
ORDER BY processing_timestamp;

-- Export to a Kafka topic for re-processing
-- (use a Kafka Connect JDBC Source connector or custom script)
```

### 11.2 Kafka DLQ Replay

Use Kafka's `kafka-consumer-groups.sh --reset-offsets` to replay from the `dlq` topic's beginning or a specific offset into a new input topic processed by a remediation pipeline.

---

## 12. Capacity Planning

### 12.1 Target: 50,000 EPS

| Resource | Calculation | Setting |
|---|---|---|
| Kafka partitions (input) | 50,000 EPS / 4,000 per partition = 12.5 → **12** | 12 partitions per input topic |
| Flink task slots | Match partition count → **12** | `parallelism.default: 12` |
| TaskManager count | 12 slots / 4 slots-per-TM = **3 TMs** | `taskManager.replicas: 3` |
| TM memory | 8 GB per TM (6 GB heap + 2 GB RocksDB off-heap) | `memory: 8192m` |
| PostgreSQL connections | 12 slots × 20 pool max = **240 conn** | `max_connections=300` in PG |
| Checkpoint frequency | Every 60 sec at 50K EPS = ~3M events per checkpoint | `checkpointing.interval: 60000` |

### 12.2 Horizontal Scaling Beyond 50K EPS

1. Increase Kafka topic partitions (requires partition reassignment, plan carefully).
2. Increase `parallelism.default` to match new partition count.
3. Add TaskManager replicas proportionally.
4. Scale PostgreSQL with PgBouncer connection pooler in front.
5. For RiskDetector state: monitor RocksDB state size; migrate to larger nodes if state grows beyond 10 GB.

---

## 13. CI/CD Pipeline Integration

### 13.1 Recommended CI Stages

```yaml
# .github/workflows/pipeline-ci.yml (example)
stages:
  - name: compile
    run: mvn clean compile

  - name: unit-tests
    run: mvn test
    # Runs 7 tests: 6 unit + 1 integration (DirectRunner, no services)

  - name: build-fat-jar
    run: mvn clean install -DskipTests

  - name: docker-build-push
    run: |
      docker build -f infrastructure/docker/Dockerfile.pipeline -t $REGISTRY/beam-pipeline:$SHA .
      docker push $REGISTRY/beam-pipeline:$SHA

  - name: deploy-staging
    run: |
      kubectl set image flinkdeployment/beam-ecommerce-pipeline \
        flink-main-container=$REGISTRY/beam-pipeline:$SHA \
        -n beam-platform-staging

  - name: smoke-test
    run: |
      # Wait 60s for job to restart, then check Flink REST API job status
      sleep 60
      curl -sf http://flink-jm.staging.internal:8081/jobs | jq '.jobs[] | select(.status=="RUNNING")'

  - name: deploy-production
    when: manual  # Require explicit approval for production
    run: |
      kubectl set image flinkdeployment/beam-ecommerce-pipeline \
        flink-main-container=$REGISTRY/beam-pipeline:$SHA \
        -n beam-platform
```

---

## 14. Disaster Recovery

### 14.1 RTO / RPO Targets

| Scenario | Recovery Time Objective | Recovery Point Objective |
|---|---|---|
| Single TaskManager failure | < 30 seconds (Flink auto-recovery) | 0 (in-flight state restored from checkpoint) |
| JobManager failure | < 2 minutes (HA standby takeover) | Last checkpoint (default: 60 seconds) |
| Full cluster failure | < 15 minutes | Last checkpoint in S3 |
| Kafka broker failure (1 of 3) | Transparent (replication) | 0 |
| PostgreSQL primary failure | < 5 minutes (promotion of standby) | 0 (synchronous replication) |

### 14.2 Flink High Availability

```yaml
# Add to flinkConfiguration in FlinkDeployment
high-availability: kubernetes
high-availability.storageDir: s3://your-flink-checkpoints/ha/
kubernetes.cluster-id: beam-ecommerce-pipeline
```

---

## 15. Code Changes Required for Full Production Readiness

The current codebase is a complete reference implementation. The following changes should be made before production go-live:

### 15.1 Enable Kafka TLS/SASL in KafkaSinkDoFn

Add `security.protocol`, `sasl.mechanism`, and credential properties to `KafkaSinkDoFn.initProducer()`.

### 15.2 Enable JDBC Retry

Wrap `insertAggregatedMetric()`, `insertRiskSignal()`, and `insertDlqEnvelope()` calls in `PostgresSinkDoFn` with `RetryUtil.executeWithRetry()` for transient `SQLException` failures.

### 15.3 Replace Static Catalog Side Input

Replace the `Create.of(KV.of(...))` hardcoded catalog in `StreamingPipeline.buildPipeline()` with a periodic bounded read from a PostgreSQL table or Redis cache.

### 15.4 HyperLogLog for Unique Customer Count

Replace `Set<String> uniqueCustomers` in `RevenueAggregateCombineFn.Accumulator` with a HyperLogLog sketch (use `com.clearspring.analytics:stream:2.9.8`) to eliminate cross-worker set merge overhead.

### 15.5 Kafka Source (KafkaIO.read)

The current `StreamingPipeline.main()` uses a `Create.of()` source for local dev. For production, replace with:

```java
// In StreamingPipeline.buildPipeline(), replace Create.of() source:
PCollection<String> inputStrings = pipeline
    .apply("ReadFromKafka", KafkaIO.<String, String>read()
        .withBootstrapServers(config.getKafkaBootstrapServers())
        .withTopics(Arrays.asList(
            config.getKafkaTopicOrders(),
            config.getKafkaTopicPayments(),
            config.getKafkaTopicCustomer()))
        .withKeyDeserializer(StringDeserializer.class)
        .withValueDeserializer(StringDeserializer.class)
        .withConsumerConfigUpdates(ImmutableMap.of(
            ConsumerConfig.GROUP_ID_CONFIG, config.getKafkaGroupId(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
        .withReadCommitted()  // Read only committed transactions
        .withoutMetadata())
    .apply("ExtractValues", Values.create());
```

---

## Summary Checklist for Production Go-Live

- [ ] Kubernetes cluster provisioned with required node sizes
- [ ] Cert-Manager and Flink Kubernetes Operator installed
- [ ] Container image built, scanned (Trivy/Snyk), and pushed to registry
- [ ] Kubernetes Secrets created (Postgres, Kafka, S3 credentials)
- [ ] Kafka topics created with correct partition counts and replication
- [ ] PostgreSQL schema applied via Flyway migration
- [ ] S3 buckets created with lifecycle policies and IAM policies
- [ ] FlinkDeployment CRD applied and job status = RUNNING
- [ ] Prometheus scraping Flink metrics on port 9249
- [ ] Grafana dashboard configured with key metric panels
- [ ] Alerts configured in AlertManager for critical metrics
- [ ] OpenTelemetry traces flowing to Jaeger/Tempo
- [ ] DLQ Kafka topic retention set to 30 days
- [ ] Flink HA configured with S3-backed metadata
- [ ] CI/CD pipeline configured with manual production approval gate
- [ ] Load test run at 50K EPS (simulator at 50K rate against staging cluster)
- [ ] Runbook documented in Confluence / Notion for on-call team
