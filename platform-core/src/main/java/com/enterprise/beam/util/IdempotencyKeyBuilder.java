package com.enterprise.beam.util;

import java.time.Instant;

/**
 * Helper class for constructing unique database upsert conflict keys.
 */
public class IdempotencyKeyBuilder {

  private IdempotencyKeyBuilder() {
  }

  public static String buildMetricKey(Instant windowStart, String windowType, String region) {
    return String.format("%s_%s_%s", windowStart.toString(), windowType, region);
  }

  public static String buildSessionKey(String customerId, Instant sessionStart) {
    return String.format("%s_%s", customerId, sessionStart.toString());
  }

  /**
   * Constructs idempotency key for customer risk signals: customerId|signalType|escalated|minuteBucket.
   */
  public static String buildRiskSignalKey(String customerId, String signalType, boolean escalated, long minuteBucket) {
    return String.format("%s|%s|%b|%d", customerId, signalType, escalated, minuteBucket);
  }

  /**
   * Constructs idempotency key for cart abandonment signals: customerId|CART|windowStart.
   */
  public static String buildCartAbandonmentKey(String customerId, Instant windowStart) {
    return String.format("%s|CART|%s", customerId, windowStart != null ? windowStart.toString() : "");
  }

  /**
   * Constructs idempotency key for dead-letter queue records:
   * SHA-256 hex of pipeline_stage|kafka_topic|kafka_partition|kafka_offset|original_payload.
   */
  public static String buildDlqKey(
      String pipelineStage,
      String kafkaTopic,
      int kafkaPartition,
      long kafkaOffset,
      String originalPayload) {
    String raw = String.format("%s|%s|%d|%d|%s",
        pipelineStage != null ? pipelineStage : "",
        kafkaTopic != null ? kafkaTopic : "",
        kafkaPartition,
        kafkaOffset,
        originalPayload != null ? originalPayload : "");
    try {
      java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(raw.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder(digest.length * 2);
      for (byte b : digest) {
        hex.append(String.format("%02x", b));
      }
      return hex.toString();
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 digest algorithm not available", e);
    }
  }
}
