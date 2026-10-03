package com.enterprise.beam.pipeline;

import com.enterprise.beam.util.JsonUtils;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

/**
 * Extracts the event timestamp for KafkaIO records.
 *
 * <p>Fallback hierarchy (T2.3):
 * <ol>
 *   <li>{@code event_time} field in JSON payload (preferred — highest precision)
 *   <li>Kafka record timestamp (broker or producer-assigned)
 *   <li>Joda EPOCH (0 ms) — deterministic fallback; the record will be treated as very-late
 *       and ValidateEventFn routes it to DLQ via {@code ERR_EVENT_TIME_OUT_OF_RANGE}.
 *       Never use {@code Instant.now()} — that makes the watermark non-reproducible.
 * </ol>
 */
class KafkaTimestampExtractor
    implements SerializableFunction<KafkaRecord<String, String>, org.joda.time.Instant> {

  private static final long serialVersionUID = 1L;

  @Override
  public org.joda.time.Instant apply(KafkaRecord<String, String> record) {
    long kafkaTs = record.getTimestamp();
    String val = record.getKV() != null ? record.getKV().getValue() : null;
    if (val != null) {
      try {
        com.fasterxml.jackson.databind.JsonNode node = JsonUtils.getMapper().readTree(val);
        com.fasterxml.jackson.databind.JsonNode eventTimeNode = node.get("event_time");
        if (eventTimeNode != null && !eventTimeNode.isNull() && eventTimeNode.isTextual()) {
          java.time.Instant parsed = java.time.Instant.parse(eventTimeNode.asText());
          return new org.joda.time.Instant(parsed.toEpochMilli());
        }
      } catch (Exception ignored) {
      }
    }
    // Fallback hierarchy — see class Javadoc and docs/DESIGN.md "Known Behaviours".
    return kafkaTs > 0 ? new org.joda.time.Instant(kafkaTs) : new org.joda.time.Instant(0L);
  }
}
