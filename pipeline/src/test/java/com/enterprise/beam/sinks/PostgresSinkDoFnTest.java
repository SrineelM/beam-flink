package com.enterprise.beam.sinks;

import com.enterprise.beam.config.PipelineConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit tests for {@link PostgresSinkDoFn} SQL correctness and reconnect throttle (A4).
 *
 * <p>Verifies T1.3: the upsert SQL must include a pane_index guard to prevent
 * replayed older panes from overwriting newer cumulative rows.
 *
 * <p>Verifies A4: fail-open reconnect storm is throttled to at most once per
 * {@link PostgresSinkDoFn#RECONNECT_THROTTLE_MS} milliseconds.
 */
public class PostgresSinkDoFnTest {

  /**
   * T1.3: The upsert SQL must include the WHERE clause guard
   * {@code WHERE EXCLUDED.pane_index >= aggregated_metrics.pane_index}.
   */
  @Test
  public void testUpsertSqlContainsPaneIndexGuard() {
    String sql = PostgresSinkDoFn.UPSERT_AGGREGATED_METRIC_SQL;
    Assertions.assertNotNull(sql, "UPSERT_AGGREGATED_METRIC_SQL must not be null");
    Assertions.assertTrue(
        sql.contains("EXCLUDED.pane_index >= aggregated_metrics.pane_index"),
        "Upsert SQL must contain the pane_index guard to prevent older panes from overwriting newer rows. "
            + "Actual SQL: " + sql);
  }

  /**
   * T1.3: The upsert SQL must use ON CONFLICT DO UPDATE with the expected conflict key columns.
   * The conflict key is (window_start, window_type, region) per the schema.
   */
  @Test
  public void testUpsertSqlConflictTarget() {
    String sql = PostgresSinkDoFn.UPSERT_AGGREGATED_METRIC_SQL;
    Assertions.assertTrue(
        sql.contains("ON CONFLICT (window_start, window_type, region) DO UPDATE SET"),
        "Upsert SQL must have ON CONFLICT target on (window_start, window_type, region). "
            + "Actual SQL: " + sql);
  }

  /**
   * T1.3: The upsert SQL must include pane_index in both INSERT columns and UPDATE SET.
   */
  @Test
  public void testUpsertSqlIncludesPaneIndexInInsertAndUpdate() {
    String sql = PostgresSinkDoFn.UPSERT_AGGREGATED_METRIC_SQL;
    // Count "pane_index" occurrences: should appear in INSERT columns, Values params,
    // DO UPDATE SET, and WHERE clause — at least 4 times.
    long occurrences = countOccurrences(sql, "pane_index");
    Assertions.assertTrue(occurrences >= 4,
        "pane_index must appear at least 4 times in the upsert SQL "
            + "(INSERT, VALUES, UPDATE SET, WHERE guard). Actual occurrences: " + occurrences);
  }

  /**
   * A4: After a failed init attempt, a second call within RECONNECT_THROTTLE_MS must
   * be skipped (throttled). Only one actual init attempt should occur per interval.
   */
  @Test
  public void testInitDataSourceIsThrottledInFailOpenMode() throws Exception {
    PipelineConfig config = new PipelineConfig();
    config.setEnvironment("LOCAL");
    config.setSinkFailOpen(true);
    // Use an unreachable URL to guarantee initDataSource() always fails
    config.setPostgresUrl("jdbc:postgresql://localhost:19999/nonexistent");

    PostgresSinkDoFn<Object> doFn = new PostgresSinkDoFn<>(config);

    // Inject a clock that always returns the same value (frozen time)
    AtomicLong frozenClock = new AtomicLong(1_000_000L);
    setField(doFn, "clockMs", (java.util.function.LongSupplier) frozenClock::get);

    // First call: triggers initDataSource(); since postgres is unreachable,
    // it fails and records lastInitAttemptMs = frozenClock value
    try {
      doFn.setup();
    } catch (Exception ignored) {}

    long lastAttemptMs = (long) getField(doFn, "lastInitAttemptMs");
    Assertions.assertEquals(1_000_000L, lastAttemptMs,
        "A4: lastInitAttemptMs must be set to clock value after failed init");

    // Second call within throttle window (clock still frozen): should be skipped
    // We test by verifying lastInitAttemptMs does not change (no new attempt)
    try {
      doFn.setup();
    } catch (Exception ignored) {}
    long lastAttemptMs2 = (long) getField(doFn, "lastInitAttemptMs");
    Assertions.assertEquals(lastAttemptMs, lastAttemptMs2,
        "A4: Second init within throttle window must be skipped (lastInitAttemptMs unchanged)");

    // Advance clock beyond throttle window: a new attempt must be allowed
    frozenClock.set(1_000_000L + PostgresSinkDoFn.RECONNECT_THROTTLE_MS + 1);
    try {
      doFn.setup();
    } catch (Exception ignored) {}
    long lastAttemptMs3 = (long) getField(doFn, "lastInitAttemptMs");
    Assertions.assertEquals(frozenClock.get(), lastAttemptMs3,
        "A4: After throttle expires, a new attempt must be recorded");
  }

  /**
   * A4: Verify RECONNECT_THROTTLE_MS constant is 30 seconds.
   */
  @Test
  public void testReconnectThrottleConstantIs30Seconds() {
    Assertions.assertEquals(30_000L, PostgresSinkDoFn.RECONNECT_THROTTLE_MS,
        "RECONNECT_THROTTLE_MS must be 30 seconds (30000 ms)");
  }

  private long countOccurrences(String text, String substring) {
    int count = 0;
    int idx = 0;
    while ((idx = text.indexOf(substring, idx)) != -1) {
      count++;
      idx += substring.length();
    }
    return count;
  }

  private void setField(Object obj, String fieldName, Object value) throws Exception {
    Field field = obj.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(obj, value);
  }

  private Object getField(Object obj, String fieldName) throws Exception {
    Field field = obj.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(obj);
  }
}
