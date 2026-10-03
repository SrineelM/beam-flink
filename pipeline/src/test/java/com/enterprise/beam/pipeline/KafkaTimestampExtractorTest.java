package com.enterprise.beam.pipeline;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaTimestampType;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * Unit tests for {@link KafkaTimestampExtractor}.
 *
 * <p>T2.3: Verifies the three-tier timestamp fallback hierarchy:
 * <ol>
 *   <li>{@code event_time} field in JSON payload — highest precision, preferred
 *   <li>Kafka record timestamp ({@code kafkaTs > 0}) — broker or producer-assigned
 *   <li>Joda EPOCH (0 ms) — deterministic last resort, produces a very-late element
 * </ol>
 */
public class KafkaTimestampExtractorTest {

  private final KafkaTimestampExtractor extractor = new KafkaTimestampExtractor();

  /** Build a KafkaRecord with specified JSON payload and Kafka record timestamp. */
  private KafkaRecord<String, String> record(String jsonPayload, long kafkaTimestampMs) {
    return new KafkaRecord<String, String>(
        "test-topic",
        0,
        0L,
        kafkaTimestampMs,
        KafkaTimestampType.CREATE_TIME,
        new RecordHeaders(),
        KV.of(null, jsonPayload));
  }

  /**
   * T2.3 (tier 1): event_time in JSON payload overrides Kafka record timestamp.
   */
  @Test
  public void testEventTimeFromJsonPayloadTakesPriority() {
    long kafkaTs = System.currentTimeMillis();
    long expectedTs = kafkaTs - 30_000L; // 30 seconds earlier than Kafka timestamp
    String eventTime = java.time.Instant.ofEpochMilli(expectedTs)
        .atZone(ZoneOffset.UTC).toInstant().toString();
    String json = "{\"event_id\":\"abc\",\"event_type\":\"ORDER_CREATED\","
        + "\"event_time\":\"" + eventTime + "\"}";

    Instant result = extractor.apply(record(json, kafkaTs));

    Assertions.assertEquals(expectedTs, result.getMillis(),
        "T2.3: event_time in JSON payload must take priority over Kafka record timestamp");
  }

  /**
   * T2.3 (tier 2): When event_time is absent, fall back to Kafka record timestamp.
   */
  @Test
  public void testFallsBackToKafkaTimestampWhenNoEventTimeField() {
    long kafkaTs = System.currentTimeMillis() - 5000L;
    String jsonWithoutEventTime = "{\"event_id\":\"abc\",\"event_type\":\"ORDER_CREATED\"}";

    Instant result = extractor.apply(record(jsonWithoutEventTime, kafkaTs));

    Assertions.assertEquals(kafkaTs, result.getMillis(),
        "T2.3: Must fall back to Kafka record timestamp when event_time is absent from payload");
  }

  /**
   * T2.3 (tier 2): Malformed JSON — falls back to Kafka record timestamp gracefully.
   */
  @Test
  public void testFallsBackToKafkaTimestampOnMalformedJson() {
    long kafkaTs = System.currentTimeMillis() - 1000L;
    String malformed = "{not-valid-json!!}";

    Instant result = extractor.apply(record(malformed, kafkaTs));

    Assertions.assertEquals(kafkaTs, result.getMillis(),
        "T2.3: Malformed JSON must fall back to Kafka record timestamp without throwing");
  }

  /**
   * T2.3 (tier 3): When Kafka timestamp is 0 or negative AND there is no event_time,
   * fall back to Joda EPOCH (0 ms) — a deterministic value rather than Instant.now().
   * This makes the resulting element very-late; ValidateEventFn will DLQ it.
   */
  @Test
  public void testFallsBackToEpochWhenKafkaTimestampIsZero() {
    String jsonWithoutEventTime = "{\"event_id\":\"abc\",\"event_type\":\"ORDER_CREATED\"}";

    Instant result = extractor.apply(record(jsonWithoutEventTime, 0L));

    Assertions.assertEquals(0L, result.getMillis(),
        "T2.3: When kafkaTs <= 0 and no event_time, must fall back to EPOCH (0 ms), "
            + "not Instant.now() which would make watermarks non-reproducible");
  }

  /**
   * T2.3 (tier 3): Negative Kafka timestamp also falls back to EPOCH.
   */
  @Test
  public void testFallsBackToEpochWhenKafkaTimestampIsNegative() {
    String jsonWithoutEventTime = "{\"event_id\":\"abc\"}";

    Instant result = extractor.apply(record(jsonWithoutEventTime, -1L));

    Assertions.assertEquals(0L, result.getMillis(),
        "T2.3: Negative kafkaTs must also fall back to EPOCH (0 ms)");
  }

  /**
   * T2.3: Null JSON payload (e.g. tombstone record) falls back to Kafka timestamp if > 0.
   */
  @Test
  public void testNullPayloadFallsBackToKafkaTimestamp() {
    long kafkaTs = System.currentTimeMillis();
    KafkaRecord<String, String> nullPayloadRecord = new KafkaRecord<String, String>(
        "test-topic", 0, 0L, kafkaTs, KafkaTimestampType.CREATE_TIME,
        new RecordHeaders(), KV.of(null, null));

    Instant result = extractor.apply(nullPayloadRecord);

    Assertions.assertEquals(kafkaTs, result.getMillis(),
        "T2.3: Null payload must fall back to Kafka record timestamp");
  }
}
