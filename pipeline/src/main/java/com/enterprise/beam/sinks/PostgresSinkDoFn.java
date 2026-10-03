package com.enterprise.beam.sinks;

import com.enterprise.beam.config.PipelineConfig;
import com.enterprise.beam.model.AggregatedMetric;
import com.enterprise.beam.model.DlqEnvelope;
import com.enterprise.beam.model.RiskSignal;
import com.enterprise.beam.util.IdempotencyKeyBuilder;
import com.enterprise.beam.util.RetryUtil;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.sql.Timestamp;

/**
 * Thread-safe DoFn for writing aggregated metrics, risk signals, and DLQ envelopes to PostgreSQL.
 * Uses HikariCP connection pool for resource efficiency on local laptop and enterprise clusters.
 */
public class PostgresSinkDoFn<T> extends DoFn<T, Void> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(PostgresSinkDoFn.class);

  /**
   * Upsert SQL for aggregated_metrics. The {@code WHERE} clause prevents a replayed or
   * reordered older pane from overwriting a newer cumulative row: only update when the
   * incoming pane_index is greater than or equal to the stored one.
   */
  static final String UPSERT_AGGREGATED_METRIC_SQL = "INSERT INTO aggregated_metrics ("
      + "window_start, window_end, window_type, region, event_type, "
      + "gross_revenue, cancelled_revenue, total_revenue, order_count, avg_order_value, "
      + "failed_payment_count, failed_payment_rate, cancellation_count, unique_customers, "
      + "pane_index, updated_at"
      + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) "
      + "ON CONFLICT (window_start, window_type, region) DO UPDATE SET "
      + "window_end = EXCLUDED.window_end, "
      + "event_type = EXCLUDED.event_type, "
      + "gross_revenue = EXCLUDED.gross_revenue, "
      + "cancelled_revenue = EXCLUDED.cancelled_revenue, "
      + "total_revenue = EXCLUDED.total_revenue, "
      + "order_count = EXCLUDED.order_count, "
      + "avg_order_value = EXCLUDED.avg_order_value, "
      + "failed_payment_count = EXCLUDED.failed_payment_count, "
      + "failed_payment_rate = EXCLUDED.failed_payment_rate, "
      + "cancellation_count = EXCLUDED.cancellation_count, "
      + "unique_customers = EXCLUDED.unique_customers, "
      + "pane_index = EXCLUDED.pane_index, "
      + "updated_at = CURRENT_TIMESTAMP "
      + "WHERE EXCLUDED.pane_index >= aggregated_metrics.pane_index";

  private final PipelineConfig config;
  private transient HikariDataSource dataSource;
  /** Millis of last init attempt; 0 = never attempted. Used to throttle reconnects in fail-open mode. */
  private transient volatile long lastInitAttemptMs = 0L;
  /** Reconnect interval for fail-open mode: at most once per 30 seconds. */
  static final long RECONNECT_THROTTLE_MS = 30_000L;
  /** Injectable clock supplier for testing. */
  private transient java.util.function.LongSupplier clockMs = System::currentTimeMillis;

  public PostgresSinkDoFn(PipelineConfig config) {
    this.config = config;
  }

  @Setup
  public void setup() {
    initDataSource();
  }

  private synchronized void initDataSource() {
    if (dataSource != null && !dataSource.isClosed()) {
      return;
    }
    // A4: In fail-open mode, throttle reconnect attempts to once per RECONNECT_THROTTLE_MS.
    // This prevents a 5-second connection timeout per element when Postgres is offline.
    long nowMs = clockMs.getAsLong();
    if (config.isSinkFailOpen() && "LOCAL".equalsIgnoreCase(config.getEnvironment())) {
      if (lastInitAttemptMs > 0 && (nowMs - lastInitAttemptMs) < RECONNECT_THROTTLE_MS) {
        return; // throttled: too soon since last attempt
      }
      lastInitAttemptMs = nowMs;
    }
    try {
      HikariConfig hikariConfig = new HikariConfig();
      hikariConfig.setJdbcUrl(config.getPostgresUrl());
      hikariConfig.setUsername(config.getPostgresUser());
      hikariConfig.setPassword(config.getPostgresPassword());
      hikariConfig.setMinimumIdle(config.getPostgresPoolMin());
      hikariConfig.setMaximumPoolSize(config.getPostgresPoolMax());
      hikariConfig.setConnectionTimeout(5000);
      hikariConfig.setDriverClassName("org.postgresql.Driver");

      dataSource = new HikariDataSource(hikariConfig);
      lastInitAttemptMs = 0L; // reset throttle on success
      LOG.info("HikariCP dataSource initialized for JDBC URL: {}", config.getPostgresUrl());
    } catch (Exception e) {
      if (config.isSinkFailOpen() && "LOCAL".equalsIgnoreCase(config.getEnvironment())) {
        LOG.warn("[fail-open] PostgreSQL init failed; will retry after {}s: {}",
            RECONNECT_THROTTLE_MS / 1000, e.getMessage());
      } else {
        LOG.error("Could not initialize HikariCP pool for PostgreSQL: {}", e.getMessage(), e);
        throw new RuntimeException("Failed to initialize PostgreSQL connection pool: " + e.getMessage(), e);
      }
    }
  }

  /**
   * Determines whether an exception represents a transient database failure suitable for retry.
   */
  public static boolean isTransientSqlException(Exception e) {
    if (e instanceof SQLTransientException) {
      return true;
    }
    if (e instanceof SQLException) {
      String sqlState = ((SQLException) e).getSQLState();
      if (sqlState != null && (sqlState.startsWith("08") || sqlState.startsWith("40"))) {
        return true;
      }
    }
    Throwable cause = e.getCause();
    if (cause instanceof Exception && cause != e) {
      return isTransientSqlException((Exception) cause);
    }
    return false;
  }

  @ProcessElement
  public void processElement(@Element T element) {
    if (dataSource == null) {
      initDataSource();
    }
    if (dataSource == null) {
      if (config.isSinkFailOpen() && "LOCAL".equalsIgnoreCase(config.getEnvironment())) {
        LOG.warn("Skipping PostgreSQL write because database connection pool is not available.");
        return;
      }
      throw new IllegalStateException("PostgreSQL connection pool is not available.");
    }

    try {
      // RetryUtil wraps the insert with exponential backoff (max 3 attempts, 200-2000ms, jitter=0.3).
      // Only transient failures (SQLTransientException or SQLState 08/40) are retried.
      RetryUtil.executeWithRetry(() -> {
        try (Connection conn = dataSource.getConnection()) {
          if (element instanceof AggregatedMetric) {
            insertAggregatedMetric(conn, (AggregatedMetric) element);
          } else if (element instanceof RiskSignal) {
            insertRiskSignal(conn, (RiskSignal) element);
          } else if (element instanceof DlqEnvelope) {
            insertDlqEnvelope(conn, (DlqEnvelope) element);
          }
        }
        return null;
      }, 3, 200L, 2000L, 0.3, PostgresSinkDoFn::isTransientSqlException);
    } catch (Exception e) {
      LOG.error("Failed to insert record into PostgreSQL after retries: {}", e.getMessage(), e);
      throw new RuntimeException("PostgreSQL sink insert failed: " + e.getMessage(), e);
    }
  }

  private void insertAggregatedMetric(Connection conn, AggregatedMetric m) throws Exception {
    try (PreparedStatement ps = conn.prepareStatement(UPSERT_AGGREGATED_METRIC_SQL)) {
      ps.setTimestamp(1, m.getWindowStart() != null ? java.sql.Timestamp.from(m.getWindowStart()) : null);
      ps.setTimestamp(2, m.getWindowEnd() != null ? java.sql.Timestamp.from(m.getWindowEnd()) : null);
      ps.setString(3, m.getWindowType());
      ps.setString(4, m.getRegion() != null ? m.getRegion() : "UNKNOWN");
      ps.setString(5, m.getEventType());
      ps.setBigDecimal(6, m.getGrossRevenue());
      ps.setBigDecimal(7, m.getCancelledRevenue());
      ps.setBigDecimal(8, m.getTotalRevenue());
      ps.setLong(9, m.getOrderCount());
      ps.setBigDecimal(10, m.getAvgOrderValue());
      ps.setLong(11, m.getFailedPaymentCount());
      ps.setDouble(12, m.getFailedPaymentRate());
      ps.setLong(13, m.getCancellationCount());
      ps.setLong(14, m.getUniqueCustomers());
      ps.setInt(15, m.getPaneIndex());
      ps.executeUpdate();
    }
  }

  private void insertRiskSignal(Connection conn, RiskSignal s) throws Exception {
    String sql = "INSERT INTO risk_signals ("
        + "customer_id, risk_score, payment_failure_count, order_attempt_count, "
        + "total_amount_last_1hr, signal_timestamp, escalated, signal_type, signal_key"
        + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
        + "ON CONFLICT (signal_key) DO NOTHING";

    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, s.getCustomerId());
      ps.setDouble(2, s.getRiskScore());
      ps.setInt(3, s.getPaymentFailureCount());
      ps.setInt(4, s.getOrderAttemptCount());
      ps.setBigDecimal(5, s.getTotalAmountLast1Hour());
      ps.setTimestamp(6, s.getSignalTimestamp() != null ? java.sql.Timestamp.from(s.getSignalTimestamp()) : null);
      ps.setBoolean(7, s.isEscalated());
      ps.setString(8, s.getSignalType() != null ? s.getSignalType() : "FRAUD_RISK");
      ps.setString(9, s.getSignalKey());
      ps.executeUpdate();
    }
  }

  private void insertDlqEnvelope(Connection conn, DlqEnvelope d) throws Exception {
    String sql = "INSERT INTO dlq_events ("
        + "correlation_id, original_payload, error_category, error_code, "
        + "error_message, stack_trace, pipeline_stage, processing_timestamp, "
        + "kafka_topic, kafka_partition, kafka_offset, dlq_key"
        + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        + "ON CONFLICT (dlq_key) DO NOTHING";

    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, d.getCorrelationId() != null ? d.getCorrelationId().toString() : null);
      ps.setString(2, d.getOriginalPayload());
      ps.setString(3, d.getErrorCategory() != null ? d.getErrorCategory().name() : null);
      ps.setString(4, d.getErrorCode());
      ps.setString(5, d.getErrorMessage());
      ps.setString(6, d.getStackTrace());
      ps.setString(7, d.getPipelineStage());
      ps.setTimestamp(8, d.getProcessingTimestamp() != null ? java.sql.Timestamp.from(d.getProcessingTimestamp()) : null);
      ps.setString(9, d.getKafkaTopic());
      ps.setInt(10, d.getKafkaPartition());
      ps.setLong(11, d.getKafkaOffset());
      String dlqKey = d.getDlqKey() != null
          ? d.getDlqKey()
          : IdempotencyKeyBuilder.buildDlqKey(
              d.getPipelineStage(),
              d.getKafkaTopic(),
              d.getKafkaPartition(),
              d.getKafkaOffset(),
              d.getOriginalPayload());
      ps.setString(12, dlqKey);
      ps.executeUpdate();
    }
  }

  @Teardown
  public void teardown() {
    if (dataSource != null && !dataSource.isClosed()) {
      dataSource.close();
      LOG.info("HikariCP connection pool closed.");
    }
  }
}
