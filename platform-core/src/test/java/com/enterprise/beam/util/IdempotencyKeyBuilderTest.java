package com.enterprise.beam.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

/**
 * Unit tests for IdempotencyKeyBuilder.
 */
public class IdempotencyKeyBuilderTest {

  @Test
  public void testBuildMetricKey() {
    Instant start = Instant.parse("2026-08-19T10:00:00Z");
    String key = IdempotencyKeyBuilder.buildMetricKey(start, "TUMBLING_5MIN", "US-EAST");
    Assertions.assertEquals("2026-08-19T10:00:00Z_TUMBLING_5MIN_US-EAST", key);
  }

  @Test
  public void testBuildSessionKey() {
    Instant start = Instant.parse("2026-08-19T10:00:00Z");
    String key = IdempotencyKeyBuilder.buildSessionKey("CUST-100", start);
    Assertions.assertEquals("CUST-100_2026-08-19T10:00:00Z", key);
  }

  @Test
  public void testBuildRiskSignalKey() {
    String key = IdempotencyKeyBuilder.buildRiskSignalKey("CUST-100", "FRAUD_RISK", true, 29780000L);
    Assertions.assertEquals("CUST-100|FRAUD_RISK|true|29780000", key);
  }

  @Test
  public void testBuildCartAbandonmentKey() {
    Instant start = Instant.parse("2026-08-19T10:00:00Z");
    String key = IdempotencyKeyBuilder.buildCartAbandonmentKey("CUST-100", start);
    Assertions.assertEquals("CUST-100|CART|2026-08-19T10:00:00Z", key);
  }

  @Test
  public void testBuildDlqKeyDeterministic() {
    String key1 = IdempotencyKeyBuilder.buildDlqKey("ParseEventFn", "orders", 0, 123L, "{\"foo\":\"bar\"}");
    String key2 = IdempotencyKeyBuilder.buildDlqKey("ParseEventFn", "orders", 0, 123L, "{\"foo\":\"bar\"}");
    Assertions.assertNotNull(key1);
    Assertions.assertEquals(64, key1.length()); // SHA-256 is 64 hex chars
    Assertions.assertEquals(key1, key2);

    String differentKey = IdempotencyKeyBuilder.buildDlqKey("ParseEventFn", "orders", 0, 124L, "{\"foo\":\"bar\"}");
    Assertions.assertNotEquals(key1, differentKey);
  }
}
