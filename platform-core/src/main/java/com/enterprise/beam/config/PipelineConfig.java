package com.enterprise.beam.config;

import java.io.Serializable;

/**
 * Environment configuration loaded from environment variables with sensible defaults.
 *
 * <p>Contains dual-target settings for 8GB Mac (laptop) vs Enterprise Kubernetes cluster.
 *
 * <p><strong>Serialization requirement:</strong> This class is {@code Serializable} because
 * Beam serializes DoFn constructor arguments to each worker JVM. All fields must be
 * serializable primitives or Strings. Non-serializable resources (JDBC DataSource, Kafka
 * Producer) must be declared {@code transient} and re-initialized lazily in {@code @Setup}.
 *
 * <p><strong>Configuration pattern:</strong> Environment variables take precedence over
 * defaults. This allows the same JAR to run on a developer laptop (using defaults) and
 * on a Kubernetes cluster (env vars injected from Secrets) without recompilation.
 */
public class PipelineConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  // ── Kafka ──────────────────────────────────────────────────────────────────
  private String kafkaBootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
  private String kafkaGroupId = getEnv("KAFKA_GROUP_ID", "beam-ecommerce-consumer");
  private String kafkaTopicOrders = getEnv("KAFKA_TOPIC_ORDERS", "orders");
  private String kafkaTopicPayments = getEnv("KAFKA_TOPIC_PAYMENTS", "payments");
  private String kafkaTopicCustomer = getEnv("KAFKA_TOPIC_CUSTOMER", "customer-events");
  private String kafkaTopicDlq = getEnv("KAFKA_TOPIC_DLQ", "dlq");
  private String kafkaTopicAlerts = getEnv("KAFKA_TOPIC_ALERTS", "alerts");
  private String kafkaTopicMetricsOutput = getEnv("KAFKA_TOPIC_METRICS", "metrics-output");
  private String kafkaTopicCartAbandonment = getEnv("KAFKA_TOPIC_CART_ABANDONMENT", "cart-abandonment");
  // T1.7: Producer timeout settings. LOCAL defaults are intentionally small so tests fail fast.
  // Non-LOCAL defaults match typical broker SLA (delivery 120s, request 30s, block 60s).
  private long kafkaProducerDeliveryTimeoutMs =
      getEnvLong("KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS", isLocalEnv() ? 5000L : 120000L);
  private long kafkaProducerRequestTimeoutMs =
      getEnvLong("KAFKA_PRODUCER_REQUEST_TIMEOUT_MS", isLocalEnv() ? 3000L : 30000L);
  private long kafkaProducerMaxBlockMs =
      getEnvLong("KAFKA_PRODUCER_MAX_BLOCK_MS", isLocalEnv() ? 3000L : 60000L);
  // T1.8: Consumer offset reset. LOCAL defaults to earliest (replay full topic in dev);
  // non-LOCAL defaults to latest to avoid reprocessing on restart.
  private String kafkaAutoOffsetReset =
      getEnv("KAFKA_AUTO_OFFSET_RESET", isLocalEnv() ? "earliest" : "latest");

  // ── PostgreSQL ─────────────────────────────────────────────────────────────
  private String postgresUrl = getEnv("POSTGRES_URL", "jdbc:postgresql://localhost:5432/ecommerce");
  private String postgresUser = getEnv("POSTGRES_USER", "postgres");
  private String postgresPassword = getEnv("POSTGRES_PASSWORD", "postgres");
  private int postgresPoolMin = getEnvInt("POSTGRES_POOL_MIN", 2);
  private int postgresPoolMax = getEnvInt("POSTGRES_POOL_MAX", 5);

  // ── Object Storage ─────────────────────────────────────────────────────────
  private boolean useLocalObjectStorage = getEnvBool("USE_LOCAL_OBJECT_STORAGE", true);
  private String localObjectStoragePath = getEnv("LOCAL_OBJECT_STORAGE_PATH", "./data/events");
  private String objectStorageBucket = getEnv("OBJECT_STORAGE_BUCKET", "ecommerce-events");

  // ── Flink / Pipeline tuning ────────────────────────────────────────────────
  private String checkpointDir = getEnv("CHECKPOINT_DIR", "./data/checkpoints");
  private long checkpointIntervalMs = getEnvLong("CHECKPOINT_INTERVAL_MS", 60000L);
  private int flinkParallelism = getEnvInt("FLINK_PARALLELISM", 2);
  private long watermarkIntervalMs = getEnvLong("WATERMARK_INTERVAL_MS", 1000L);
  private long watermarkMaxDelaySeconds = getEnvLong("WATERMARK_MAX_DELAY_SECONDS", 30L);
  private long allowedLatenessSeconds = getEnvLong("ALLOWED_LATENESS_SECONDS", 600L);
  private long deduplicationTtlSeconds = getEnvLong("DEDUPLICATION_TTL_SECONDS", 86400L);

  // ── Risk Detection Tuning ──────────────────────────────────────────────────
  private int riskLookbackMinutes = getEnvInt("RISK_LOOKBACK_MINUTES", 60);
  private int riskFailureThreshold = getEnvInt("RISK_FAILURE_THRESHOLD", 3);
  private int riskVelocityThreshold = getEnvInt("RISK_VELOCITY_THRESHOLD", 10);
  private double escalateAt = getEnvDouble("ESCALATE_AT", 0.8);
  private double emitAt = getEnvDouble("EMIT_AT", 0.5);

  // ── Validation ─────────────────────────────────────────────────────────────
  private int maxEventAgeDays = getEnvInt("MAX_EVENT_AGE_DAYS", 7);

  // ── Environment ────────────────────────────────────────────────────────────
  private String environment = getEnv("ENVIRONMENT", "LOCAL");
  private boolean sinkFailOpen = getEnvBool("SINK_FAIL_OPEN", false);

  // ── Kafka Security passthrough (D2) ────────────────────────────────────────
  // Only applied when non-empty. Set all five fields to enable SASL/SSL.
  private String kafkaSecurityProtocol = getEnv("KAFKA_SECURITY_PROTOCOL", "");
  private String kafkaSaslMechanism = getEnv("KAFKA_SASL_MECHANISM", "");
  private String kafkaSaslJaasConfig = getEnv("KAFKA_SASL_JAAS_CONFIG", "");
  private String kafkaSslTruststoreLocation = getEnv("KAFKA_SSL_TRUSTSTORE_LOCATION", "");
  private String kafkaSslTruststorePassword = getEnv("KAFKA_SSL_TRUSTSTORE_PASSWORD", "");

  // ── Postgres additional settings ────────────────────────────────────────────
  private int postgresWriteBatchSize = getEnvInt("POSTGRES_WRITE_BATCH_SIZE", 200);
  private String postgresSslMode = getEnv("POSTGRES_SSL_MODE", "");

  /**
   * Returns true when the ENVIRONMENT env-var is LOCAL (or unset).
   * Used only during field initialisation, before the {@code environment} field is populated.
   */
  private static boolean isLocalEnv() {
    String env = System.getenv("ENVIRONMENT");
    return env == null || env.trim().isEmpty() || "LOCAL".equalsIgnoreCase(env.trim());
  }

  public PipelineConfig() {
  }

  /**
   * Validates configuration constraints at startup.
   * When SINK_FAIL_OPEN=false, also performs fast connectivity checks.
   */
  public void validate() {
    if ("PRODUCTION".equalsIgnoreCase(environment)) {
      if ("postgres".equalsIgnoreCase(postgresPassword)) {
        throw new IllegalStateException("Production environment must not use default postgres password");
      }
      if (kafkaBootstrapServers != null && kafkaBootstrapServers.contains("localhost")) {
        throw new IllegalStateException(
            "Production environment must not use localhost for Kafka bootstrap servers: " + kafkaBootstrapServers);
      }
      if (useLocalObjectStorage) {
        throw new IllegalStateException("Production environment must not use local object storage");
      }
    }
    // A5: Fail-fast health checks when sinks must not fail silently and the environment
    // is not LOCAL. In LOCAL mode services may not be running during unit tests;
    // use SINK_FAIL_OPEN=true in that case. In DEV/STAGING/PRODUCTION the check is mandatory.
    if (!sinkFailOpen && !"LOCAL".equalsIgnoreCase(environment)) {
      checkPostgresConnectivity();
      checkKafkaConnectivity();
    }
  }

  /**
   * A5: Performs a SELECT 1 health check against the configured Postgres URL.
   * Throws {@link IllegalStateException} with a clear message if unreachable.
   */
  public void checkPostgresConnectivity() {
    try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
        postgresUrl, postgresUser, postgresPassword);
        java.sql.Statement st = conn.createStatement()) {
      st.execute("SELECT 1");
    } catch (Exception e) {
      throw new IllegalStateException(
          "[startup] Postgres health check failed for " + postgresUrl
              + ": " + e.getMessage()
              + ". Set SINK_FAIL_OPEN=true to disable this check locally.", e);
    }
  }

  /**
   * A5: Checks that at least one bootstrap broker is reachable via a short
   * TCP connect timeout. Throws {@link IllegalStateException} if unreachable.
   */
  public void checkKafkaConnectivity() {
    if (kafkaBootstrapServers == null || kafkaBootstrapServers.isEmpty()) {
      return;
    }
    String[] brokers = kafkaBootstrapServers.split(",");
    for (String broker : brokers) {
      String[] parts = broker.trim().split(":");
      if (parts.length < 2) {
        continue;
      }
      String host = parts[0].trim();
      int port;
      try {
        port = Integer.parseInt(parts[1].trim());
      } catch (NumberFormatException e) {
        continue;
      }
      try (java.net.Socket s = new java.net.Socket()) {
        s.connect(new java.net.InetSocketAddress(host, port), 3000);
        return; // at least one broker reachable
      } catch (java.io.IOException e) {
        // try next broker
      }
    }
    throw new IllegalStateException(
        "[startup] Kafka health check failed: none of the bootstrap brokers is reachable: "
            + kafkaBootstrapServers
            + ". Set SINK_FAIL_OPEN=true to disable this check locally.");
  }

  private static String getEnv(String name, String defaultValue) {
    String val = System.getenv(name);
    return (val != null && !val.trim().isEmpty()) ? val.trim() : defaultValue;
  }

  private static int getEnvInt(String name, int defaultValue) {
    String val = System.getenv(name);
    if (val != null && !val.trim().isEmpty()) {
      try {
        return Integer.parseInt(val.trim());
      } catch (NumberFormatException ignored) {}
    }
    return defaultValue;
  }

  private static long getEnvLong(String name, long defaultValue) {
    String val = System.getenv(name);
    if (val != null && !val.trim().isEmpty()) {
      try {
        return Long.parseLong(val.trim());
      } catch (NumberFormatException ignored) {}
    }
    return defaultValue;
  }

  private static double getEnvDouble(String name, double defaultValue) {
    String val = System.getenv(name);
    if (val != null && !val.trim().isEmpty()) {
      try {
        return Double.parseDouble(val.trim());
      } catch (NumberFormatException ignored) {}
    }
    return defaultValue;
  }

  private static boolean getEnvBool(String name, boolean defaultValue) {
    String val = System.getenv(name);
    if (val != null && !val.trim().isEmpty()) {
      return Boolean.parseBoolean(val.trim());
    }
    return defaultValue;
  }

  public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
  public void setKafkaBootstrapServers(String kafkaBootstrapServers) { this.kafkaBootstrapServers = kafkaBootstrapServers; }

  public String getKafkaGroupId() { return kafkaGroupId; }
  public void setKafkaGroupId(String kafkaGroupId) { this.kafkaGroupId = kafkaGroupId; }

  public String getKafkaTopicOrders() { return kafkaTopicOrders; }
  public void setKafkaTopicOrders(String kafkaTopicOrders) { this.kafkaTopicOrders = kafkaTopicOrders; }

  public String getKafkaTopicPayments() { return kafkaTopicPayments; }
  public void setKafkaTopicPayments(String kafkaTopicPayments) { this.kafkaTopicPayments = kafkaTopicPayments; }

  public String getKafkaTopicCustomer() { return kafkaTopicCustomer; }
  public void setKafkaTopicCustomer(String kafkaTopicCustomer) { this.kafkaTopicCustomer = kafkaTopicCustomer; }

  public String getKafkaTopicDlq() { return kafkaTopicDlq; }
  public void setKafkaTopicDlq(String kafkaTopicDlq) { this.kafkaTopicDlq = kafkaTopicDlq; }

  public String getKafkaTopicAlerts() { return kafkaTopicAlerts; }
  public void setKafkaTopicAlerts(String kafkaTopicAlerts) { this.kafkaTopicAlerts = kafkaTopicAlerts; }

  public String getKafkaTopicMetricsOutput() { return kafkaTopicMetricsOutput; }
  public void setKafkaTopicMetricsOutput(String kafkaTopicMetricsOutput) { this.kafkaTopicMetricsOutput = kafkaTopicMetricsOutput; }

  public String getKafkaTopicCartAbandonment() { return kafkaTopicCartAbandonment; }
  public void setKafkaTopicCartAbandonment(String kafkaTopicCartAbandonment) { this.kafkaTopicCartAbandonment = kafkaTopicCartAbandonment; }

  public String getPostgresUrl() { return postgresUrl; }
  public void setPostgresUrl(String postgresUrl) { this.postgresUrl = postgresUrl; }

  public String getPostgresUser() { return postgresUser; }
  public void setPostgresUser(String postgresUser) { this.postgresUser = postgresUser; }

  public String getPostgresPassword() { return postgresPassword; }
  public void setPostgresPassword(String postgresPassword) { this.postgresPassword = postgresPassword; }

  public int getPostgresPoolMin() { return postgresPoolMin; }
  public void setPostgresPoolMin(int postgresPoolMin) { this.postgresPoolMin = postgresPoolMin; }

  public int getPostgresPoolMax() { return postgresPoolMax; }
  public void setPostgresPoolMax(int postgresPoolMax) { this.postgresPoolMax = postgresPoolMax; }

  public boolean isUseLocalObjectStorage() { return useLocalObjectStorage; }
  public void setUseLocalObjectStorage(boolean useLocalObjectStorage) { this.useLocalObjectStorage = useLocalObjectStorage; }

  public String getLocalObjectStoragePath() { return localObjectStoragePath; }
  public void setLocalObjectStoragePath(String localObjectStoragePath) { this.localObjectStoragePath = localObjectStoragePath; }

  public String getObjectStorageBucket() { return objectStorageBucket; }
  public void setObjectStorageBucket(String objectStorageBucket) { this.objectStorageBucket = objectStorageBucket; }

  public String getCheckpointDir() { return checkpointDir; }
  public void setCheckpointDir(String checkpointDir) { this.checkpointDir = checkpointDir; }

  /**
   * Returns an absolute {@code file://} URI for the checkpoint directory.
   * Flink's checkpoint storage expects a URI; a relative path causes a runtime error.
   * Only used when ENVIRONMENT=LOCAL; non-local deployments should set CHECKPOINT_DIR
   * to an absolute URI (e.g. {@code s3://bucket/checkpoints}) via the environment.
   */
  public String getCheckpointDirUri() {
    String dir = checkpointDir;
    if (dir == null || dir.isEmpty()) {
      dir = "./data/checkpoints";
    }
    // Already a URI (has scheme)
    if (dir.contains("://")) {
      return dir;
    }
    // Resolve relative paths to absolute, normalized file:// URI.
    // normalize() eliminates ./ and ../ segments that Flink checkpoint storage cannot handle.
    java.nio.file.Path path = java.nio.file.Paths.get(dir).toAbsolutePath().normalize();
    return path.toUri().toString();
  }

  public long getCheckpointIntervalMs() { return checkpointIntervalMs; }
  public void setCheckpointIntervalMs(long checkpointIntervalMs) { this.checkpointIntervalMs = checkpointIntervalMs; }

  public int getFlinkParallelism() { return flinkParallelism; }
  public void setFlinkParallelism(int flinkParallelism) { this.flinkParallelism = flinkParallelism; }

  public long getWatermarkIntervalMs() { return watermarkIntervalMs; }
  public void setWatermarkIntervalMs(long watermarkIntervalMs) { this.watermarkIntervalMs = watermarkIntervalMs; }

  public long getWatermarkMaxDelaySeconds() { return watermarkMaxDelaySeconds; }
  public void setWatermarkMaxDelaySeconds(long watermarkMaxDelaySeconds) { this.watermarkMaxDelaySeconds = watermarkMaxDelaySeconds; }

  public long getAllowedLatenessSeconds() { return allowedLatenessSeconds; }
  public void setAllowedLatenessSeconds(long allowedLatenessSeconds) { this.allowedLatenessSeconds = allowedLatenessSeconds; }

  public long getDeduplicationTtlSeconds() { return deduplicationTtlSeconds; }
  public void setDeduplicationTtlSeconds(long deduplicationTtlSeconds) { this.deduplicationTtlSeconds = deduplicationTtlSeconds; }

  public int getRiskLookbackMinutes() { return riskLookbackMinutes; }
  public void setRiskLookbackMinutes(int riskLookbackMinutes) { this.riskLookbackMinutes = riskLookbackMinutes; }

  public int getRiskFailureThreshold() { return riskFailureThreshold; }
  public void setRiskFailureThreshold(int riskFailureThreshold) { this.riskFailureThreshold = riskFailureThreshold; }

  public int getRiskVelocityThreshold() { return riskVelocityThreshold; }
  public void setRiskVelocityThreshold(int riskVelocityThreshold) { this.riskVelocityThreshold = riskVelocityThreshold; }

  public double getEscalateAt() { return escalateAt; }
  public void setEscalateAt(double escalateAt) { this.escalateAt = escalateAt; }

  public double getEmitAt() { return emitAt; }
  public void setEmitAt(double emitAt) { this.emitAt = emitAt; }

  public String getEnvironment() { return environment; }
  public void setEnvironment(String environment) { this.environment = environment; }

  public boolean isSinkFailOpen() { return sinkFailOpen; }
  public void setSinkFailOpen(boolean sinkFailOpen) { this.sinkFailOpen = sinkFailOpen; }

  public long getKafkaProducerDeliveryTimeoutMs() { return kafkaProducerDeliveryTimeoutMs; }
  public void setKafkaProducerDeliveryTimeoutMs(long ms) { this.kafkaProducerDeliveryTimeoutMs = ms; }

  public long getKafkaProducerRequestTimeoutMs() { return kafkaProducerRequestTimeoutMs; }
  public void setKafkaProducerRequestTimeoutMs(long ms) { this.kafkaProducerRequestTimeoutMs = ms; }

  public long getKafkaProducerMaxBlockMs() { return kafkaProducerMaxBlockMs; }
  public void setKafkaProducerMaxBlockMs(long ms) { this.kafkaProducerMaxBlockMs = ms; }

  public String getKafkaAutoOffsetReset() { return kafkaAutoOffsetReset; }
  public void setKafkaAutoOffsetReset(String v) { this.kafkaAutoOffsetReset = v; }

  public int getMaxEventAgeDays() { return maxEventAgeDays; }
  public void setMaxEventAgeDays(int maxEventAgeDays) { this.maxEventAgeDays = maxEventAgeDays; }

  public String getKafkaSecurityProtocol() { return kafkaSecurityProtocol; }
  public void setKafkaSecurityProtocol(String v) { this.kafkaSecurityProtocol = v; }

  public String getKafkaSaslMechanism() { return kafkaSaslMechanism; }
  public void setKafkaSaslMechanism(String v) { this.kafkaSaslMechanism = v; }

  public String getKafkaSaslJaasConfig() { return kafkaSaslJaasConfig; }
  public void setKafkaSaslJaasConfig(String v) { this.kafkaSaslJaasConfig = v; }

  public String getKafkaSslTruststoreLocation() { return kafkaSslTruststoreLocation; }
  public void setKafkaSslTruststoreLocation(String v) { this.kafkaSslTruststoreLocation = v; }

  public String getKafkaSslTruststorePassword() { return kafkaSslTruststorePassword; }
  public void setKafkaSslTruststorePassword(String v) { this.kafkaSslTruststorePassword = v; }

  public int getPostgresWriteBatchSize() { return postgresWriteBatchSize; }
  public void setPostgresWriteBatchSize(int v) { this.postgresWriteBatchSize = v; }

  public String getPostgresSslMode() { return postgresSslMode; }
  public void setPostgresSslMode(String v) { this.postgresSslMode = v; }
}

