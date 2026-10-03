package com.enterprise.beam.transforms;

import com.enterprise.beam.model.BaseEvent;
import com.enterprise.beam.model.DlqEnvelope;
import com.enterprise.beam.model.ErrorCategory;
import com.enterprise.beam.model.RawRecord;
import com.enterprise.beam.util.IdempotencyKeyBuilder;
import com.enterprise.beam.util.JsonUtils;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;

/**
 * DoFn that parses RawRecord instances into BaseEvent domain models.
 * Preserves Kafka topic, partition, and offset metadata on both valid events and DLQ envelopes.
 */
public class ParseEventFn extends DoFn<RawRecord, BaseEvent> {

  private static final long serialVersionUID = 1L;

  // D2: Beam Metrics counters. Queryable from Flink/Dataflow UI and MetricsResults in tests.
  private static final Counter EVENTS_PARSED =
      Metrics.counter(ParseEventFn.class, "events_parsed");
  private static final Counter DLQ_PARSE_ERRORS =
      Metrics.counter(ParseEventFn.class, "dlq_parse_errors");

  public static final TupleTag<BaseEvent> VALID_TAG = new TupleTag<BaseEvent>() {};
  public static final TupleTag<DlqEnvelope> DLQ_TAG = new TupleTag<DlqEnvelope>() {};

  @ProcessElement
  public void processElement(@Element RawRecord record, MultiOutputReceiver out) {
    if (record == null) {
      return;
    }
    String rawPayload = record.getPayload();
    // Guard: Kafka can deliver empty payloads on topic compaction tombstones — skip silently
    if (rawPayload == null || rawPayload.trim().isEmpty()) {
      return;
    }

    try {
      // Jackson polymorphic deserialization: the "event_type" JSON field determines
      // which BaseEvent subclass is instantiated (OrderCreatedEvent, PaymentFailedEvent, etc.)
      BaseEvent event = JsonUtils.getMapper().readValue(rawPayload, BaseEvent.class);
      event.setKafkaTopic(record.getTopic());
      event.setKafkaPartition(record.getPartition());
      event.setKafkaOffset(record.getOffset());
      out.get(VALID_TAG).output(event);
      EVENTS_PARSED.inc();
    } catch (Exception e) {
      // Any parse failure routes to DLQ with partition metadata preserved.
      DlqEnvelope dlq = new DlqEnvelope();
      dlq.setOriginalPayload(rawPayload);
      dlq.setErrorCategory(ErrorCategory.PARSE_ERROR);
      dlq.setErrorCode("ERR_JSON_PARSE");
      dlq.setErrorMessage(e.getMessage());

      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      dlq.setStackTrace(sw.toString());

      dlq.setPipelineStage("ParseEventFn");
      dlq.setProcessingTimestamp(Instant.now());
      dlq.setKafkaTopic(record.getTopic());
      dlq.setKafkaPartition(record.getPartition());
      dlq.setKafkaOffset(record.getOffset());
      dlq.setDlqKey(IdempotencyKeyBuilder.buildDlqKey(
          "ParseEventFn",
          record.getTopic(),
          record.getPartition(),
          record.getOffset(),
          rawPayload));

      out.get(DLQ_TAG).output(dlq);
      DLQ_PARSE_ERRORS.inc();
    }
  }
}
