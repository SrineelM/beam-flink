package com.enterprise.beam.pipeline;

import com.enterprise.beam.config.PipelineConfig;
import com.enterprise.beam.model.AggregatedMetric;
import com.enterprise.beam.model.DlqEnvelope;
import com.enterprise.beam.model.RawRecord;
import com.enterprise.beam.model.RiskSignal;
import com.enterprise.beam.sinks.InMemoryPipelineSinks;
import com.enterprise.beam.transforms.ParseEventFn;
import com.enterprise.beam.transforms.ValidateEventFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class StreamingPipelineTest implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Comprehensive end-to-end TestStream test asserting all key streaming semantics:
   * 1. Malformed JSON routes to DLQ
   * 2. Duplicate event_id is dropped silently (does not inflate metrics)
   * 3. Late data accumulates into the 5-min window (final orderCount=2, grossRevenue=150)
   * 4. Multi-failure risk detection triggers a fraud signal
   * 5. Session window cart abandonment triggers a cart abandonment signal
   *
   * <p>Late-data pane boundary testing (on-time pane=1 vs late pane=2) is covered in
   * {@link LateDataWindowingTest}, which uses a simpler direct pipeline with fewer stages.
   * In the full E2E pipeline, the watermark propagates lazily through multiple DoFn stages
   * before reaching the windowing stage, causing the DirectRunner to consolidate the late
   * element into the on-time pane (T1.1 design note in DESIGN.md "Known Behaviours").
   *
   * <p>Watermark schedule:
   * <ul>
   *   <li>advanceWatermarkTo(t0+4min): midway advance, no window fires yet
   *   <li>advanceWatermarkTo(tWindowEnd+8min): fires the 5-min window (all elements accumulated)
   *   <li>addElements(orderLateJson) before that: demonstrates late-data accumulation
   *   <li>advanceWatermarkTo(tSessionEnd): closes the 30-min session window
   * </ul>
   */
  @Test
  public void testFullStreamingPipelineEndToEndWithTestStream() {
    Pipeline pipeline = Pipeline.create();
    PipelineConfig config = new PipelineConfig();

    // Compute the current 5-minute window start (aligned to now). Events use current-time
    // timestamps so they pass ValidateEventFn's MAX_EVENT_AGE_DAYS freshness check.
    long nowMillis = System.currentTimeMillis();
    long windowStartMillis = nowMillis - (nowMillis % 300_000L);
    Instant t0 = new Instant(windowStartMillis);
    Instant tEvent1 = t0.plus(Duration.standardMinutes(1));
    Instant tEvent1Dup = tEvent1.plus(Duration.standardSeconds(10));
    // Late order: same 5-min window, will be added after initial watermark advance.
    Instant tEventLate = t0.plus(Duration.standardMinutes(2));
    Instant tWindowEnd = t0.plus(Duration.standardMinutes(5));
    // Session: CUST-CART-1 adds at t0+1min; 30-min gap closes at t0+31min.
    Instant tCartAdd = t0.plus(Duration.standardMinutes(1));
    Instant tSessionEnd = t0.plus(Duration.standardMinutes(36)); // well past gap

    String malformedJson = "{\"not-valid-json";

    String order1Json = "{\"event_id\":\"00000000-0000-0000-0000-000000000001\","
        + "\"event_type\":\"ORDER_CREATED\","
        + "\"customer_id\":\"CUST-US-1\","
        + "\"region\":\"US\","
        + "\"amount\":100.00,"
        + "\"event_time\":\"" + java.time.Instant.ofEpochMilli(tEvent1.getMillis()).toString() + "\"}";

    // Duplicate order 1 with identical event_id — must be dropped by DeduplicateEventsFn
    String order1DupJson = "{\"event_id\":\"00000000-0000-0000-0000-000000000001\","
        + "\"event_type\":\"ORDER_CREATED\","
        + "\"customer_id\":\"CUST-US-1\","
        + "\"region\":\"US\","
        + "\"amount\":100.00,"
        + "\"event_time\":\"" + java.time.Instant.ofEpochMilli(tEvent1Dup.getMillis()).toString() + "\"}";

    // Late order in the same 5-min window — added after the mid-watermark advance.
    // Note: due to multi-stage DirectRunner watermark propagation, this late element may
    // accumulate with order1 in the same on-time pane. See class Javadoc and LateDataWindowingTest.
    String orderLateJson = "{\"event_id\":\"00000000-0000-0000-0000-000000000002\","
        + "\"event_type\":\"ORDER_CREATED\","
        + "\"customer_id\":\"CUST-US-2\","
        + "\"region\":\"US\","
        + "\"amount\":50.00,"
        + "\"event_time\":\"" + java.time.Instant.ofEpochMilli(tEventLate.getMillis()).toString() + "\"}";

    // 3 payment failures for customer CUST-FRAUD-1 to trigger risk detection
    String fail1 = "{\"event_id\":\"00000000-0000-0000-0000-000000000011\","
        + "\"event_type\":\"PAYMENT_FAILED\","
        + "\"customer_id\":\"CUST-FRAUD-1\","
        + "\"event_time\":\"" + java.time.Instant.ofEpochMilli(t0.plus(Duration.standardMinutes(1)).getMillis()).toString() + "\"}";
    String fail2 = "{\"event_id\":\"00000000-0000-0000-0000-000000000012\","
        + "\"event_type\":\"PAYMENT_FAILED\","
        + "\"customer_id\":\"CUST-FRAUD-1\","
        + "\"event_time\":\"" + java.time.Instant.ofEpochMilli(t0.plus(Duration.standardMinutes(2)).getMillis()).toString() + "\"}";
    String fail3 = "{\"event_id\":\"00000000-0000-0000-0000-000000000013\","
        + "\"event_type\":\"PAYMENT_FAILED\","
        + "\"customer_id\":\"CUST-FRAUD-1\","
        + "\"event_time\":\"" + java.time.Instant.ofEpochMilli(t0.plus(Duration.standardMinutes(3)).getMillis()).toString() + "\"}";

    String cartAdd = "{\"event_id\":\"00000000-0000-0000-0000-000000000021\","
        + "\"event_type\":\"PRODUCT_ADDED_TO_CART\","
        + "\"customer_id\":\"CUST-CART-1\","
        + "\"region\":\"EU\","
        + "\"amount\":25.00,"
        + "\"event_time\":\"" + java.time.Instant.ofEpochMilli(tCartAdd.getMillis()).toString() + "\"}";

    // Watermark schedule: advance to t0+4min (no window fires), then add late element,
    // then advance to tWindowEnd+8min to fire the window (all elements accumulated),
    // then tSessionEnd to close the 30-min session.
    TestStream<String> stream = TestStream.create(StringUtf8Coder.of())
        .advanceWatermarkTo(t0)
        .addElements(
            TimestampedValue.of(malformedJson, t0),
            TimestampedValue.of(order1Json, tEvent1),
            TimestampedValue.of(order1DupJson, tEvent1Dup),
            TimestampedValue.of(fail1, t0.plus(Duration.standardMinutes(1))),
            TimestampedValue.of(fail2, t0.plus(Duration.standardMinutes(2))),
            TimestampedValue.of(fail3, t0.plus(Duration.standardMinutes(3))),
            TimestampedValue.of(cartAdd, tCartAdd))
        .advanceProcessingTime(Duration.standardSeconds(5))
        .advanceWatermarkTo(t0.plus(Duration.standardMinutes(4)))
        .advanceProcessingTime(Duration.standardSeconds(5))
        // Add the late order BEFORE the watermark crosses window end.
        // It will accumulate with order1 in the window.
        .addElements(TimestampedValue.of(orderLateJson, tEventLate))
        .advanceProcessingTime(Duration.standardSeconds(5))
        // Advance past window end + allowed lateness area: window fires with both orders.
        .advanceWatermarkTo(tWindowEnd.plus(Duration.standardMinutes(8)))
        .advanceProcessingTime(Duration.standardSeconds(30))
        // Advance past 30-min session gap to close the cart abandonment session.
        .advanceWatermarkTo(tSessionEnd)
        .advanceProcessingTime(Duration.standardSeconds(30))
        .advanceWatermarkToInfinity();

    PCollection<String> inputEvents = pipeline.apply("TestInputStream", stream);
    InMemoryPipelineSinks sinks = new InMemoryPipelineSinks();
    StreamingPipeline.buildPipeline(pipeline, config, inputEvents, sinks);

    // 1. Assert DLQ has malformed record
    PAssert.that(sinks.getDlqEnvelopes()).satisfies(dlq -> {
      List<DlqEnvelope> list = new ArrayList<>();
      dlq.forEach(list::add);
      Assertions.assertTrue(list.stream().anyMatch(d -> "ParseEventFn".equals(d.getPipelineStage())
          && d.getOriginalPayload().contains("not-valid-json")),
          "Malformed JSON must be routed to DLQ with stage=ParseEventFn");
      return null;
    });

    // 2. Assert Metrics:
    //    - Duplicate dropped: dup event_id suppressed, gross revenue counts only unique order (100)
    //    - Late event accumulates in same or late pane (total count=2, grossRevenue=150)
    //    - avgOrderValue = grossRevenue(100+50) / orderCount(2) = 75.00 (T1.4)
    //
    //    Note: we assert on the FINAL accumulated state across all panes. The exact number
    //    of panes (1 or 2) depends on DirectRunner watermark propagation lag and is tested
    //    separately in LateDataWindowingTest.
    PAssert.that(sinks.getMetrics()).satisfies(metrics -> {
      // Find the pane with the highest accumulated order count for US (final state)
      AggregatedMetric finalUs = null;
      for (AggregatedMetric m : metrics) {
        if ("US".equals(m.getRegion())) {
          if (finalUs == null || m.getOrderCount() > finalUs.getOrderCount()) {
            finalUs = m;
          }
        }
      }
      Assertions.assertNotNull(finalUs, "Expected at least one AggregatedMetric for US region");
      Assertions.assertEquals(2L, finalUs.getOrderCount(),
          "Accumulated US orderCount must be 2 (order1 + orderLate, dup dropped)");
      // grossRevenue = 100 (order1) + 50 (orderLate) = 150
      Assertions.assertEquals(new BigDecimal("150.00"), finalUs.getGrossRevenue(),
          "Accumulated US grossRevenue must be 150.00");
      // T1.4: avgOrderValue = grossRevenue / orderCount = 150.00 / 2 = 75.00
      Assertions.assertEquals(new BigDecimal("75.00"), finalUs.getAvgOrderValue(),
          "avgOrderValue must use grossRevenue (T1.4): 150.00 / 2 = 75.00");
      Assertions.assertNotNull(finalUs.getWindowStart(), "windowStart must be set");
      Assertions.assertNotNull(finalUs.getWindowEnd(), "windowEnd must be set");
      return null;
    });

    // 3. Assert Fraud Signal
    PAssert.that(sinks.getRiskSignals()).satisfies(signals -> {
      List<RiskSignal> list = new ArrayList<>();
      signals.forEach(list::add);
      Assertions.assertTrue(list.stream().anyMatch(s -> "CUST-FRAUD-1".equals(s.getCustomerId())
          && "FRAUD_RISK".equals(s.getSignalType()) && s.getRiskScore() >= 0.5),
          "Expected fraud risk signal for CUST-FRAUD-1 with score >= 0.5");
      return null;
    });

    // 4. T1.1: Assert CartAbandonmentSignals — previously commented out.
    // Now enabled: watermark advances to tSessionEnd (t0+36min > tCartAdd+30min = t0+31min),
    // which closes the session window and triggers the cart abandonment signal.
    PAssert.that(sinks.getCartAbandonmentSignals()).satisfies(signals -> {
      List<RiskSignal> list = new ArrayList<>();
      signals.forEach(list::add);
      Assertions.assertTrue(list.stream().anyMatch(s -> "CUST-CART-1".equals(s.getCustomerId())
          && "CART_ABANDONMENT".equals(s.getSignalType())),
          "Expected cart abandonment signal for CUST-CART-1");
      // Verify signal_key and signalTimestamp are set (T1.1 requirement)
      list.stream()
          .filter(s -> "CUST-CART-1".equals(s.getCustomerId()))
          .findFirst()
          .ifPresent(s -> {
            Assertions.assertNotNull(s.getSignalKey(),
                "signal_key must be set on cart abandonment signal");
            Assertions.assertTrue(s.getSignalKey().startsWith("CUST-CART-1|CART|"),
                "signal_key must follow pattern customerId|CART|...");
            Assertions.assertNotNull(s.getSignalTimestamp(),
                "signalTimestamp must be set on cart abandonment signal");
          });
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  /**
   * DLQ routing assertion test for parse vs validate stages.
   * Uses event_time = now() to pass ValidateEventFn's freshness check.
   */
  @Test
  public void testDlqRoutingOnMalformedAndInvalidInput() {
    Pipeline pipeline = Pipeline.create();

    // Use current time so events pass ValidateEventFn's MAX_EVENT_AGE_DAYS (7-day) check.
    String now = java.time.Instant.now().toString();
    String malformedJson = "{not-valid-json!!}";
    // Valid JSON structure and valid event_time, but missing customer_id — should fail ValidateEventFn
    String missingCustomer = "{\"event_id\":\"ffffffff-0000-0000-0000-000000000001\","
        + "\"event_type\":\"ORDER_CREATED\",\"amount\":50.00,\"event_time\":\"" + now + "\"}";

    PCollection<RawRecord> input = pipeline.apply("CreateTestInput",
        Create.of(
            new RawRecord(malformedJson, "test-orders", 0, 1L, System.currentTimeMillis()),
            new RawRecord(missingCustomer, "test-orders", 0, 2L, System.currentTimeMillis())));

    // Stage 1: Parse
    PCollectionTuple parseTuple = input.apply("ParseEvents",
        ParDo.of(new ParseEventFn())
            .withOutputTags(ParseEventFn.VALID_TAG, TupleTagList.of(ParseEventFn.DLQ_TAG)));

    // Stage 2: Validate
    PCollectionTuple validateTuple = parseTuple.get(ParseEventFn.VALID_TAG).apply("ValidateEvents",
        ParDo.of(new ValidateEventFn())
            .withOutputTags(ValidateEventFn.VALID_TAG, TupleTagList.of(ValidateEventFn.DLQ_TAG)));

    // Merge both DLQ streams — both should contain exactly one record
    PCollection<DlqEnvelope> mergedDlq = PCollectionList
        .of(parseTuple.get(ParseEventFn.DLQ_TAG))
        .and(validateTuple.get(ValidateEventFn.DLQ_TAG))
        .apply("MergeDlq", Flatten.pCollections());

    // Verify parse DLQ: malformed JSON must land here
    PAssert.that(parseTuple.get(ParseEventFn.DLQ_TAG))
        .satisfies(new org.apache.beam.sdk.transforms.SerializableFunction<Iterable<DlqEnvelope>, Void>() {
          @Override
          public Void apply(Iterable<DlqEnvelope> envelopes) {
            DlqEnvelope e = envelopes.iterator().next();
            Assertions.assertEquals("ParseEventFn", e.getPipelineStage(),
                "Malformed JSON must route to DLQ with stage=ParseEventFn");
            Assertions.assertEquals(malformedJson, e.getOriginalPayload(),
                "Original malformed payload must be preserved for replay");
            Assertions.assertEquals("test-orders", e.getKafkaTopic());
            Assertions.assertNotNull(e.getDlqKey());
            return null;
          }
        });

    // Verify validate DLQ: missing customer_id must land here with ERR_MISSING_CUST.
    // NOTE: ValidateEventFn checks customerId (check 4) before eventTime range (check 6),
    // so even if event_time were stale, ERR_MISSING_CUST fires first (see ValidateEventFn order).
    PAssert.that(validateTuple.get(ValidateEventFn.DLQ_TAG))
        .satisfies(new org.apache.beam.sdk.transforms.SerializableFunction<Iterable<DlqEnvelope>, Void>() {
          @Override
          public Void apply(Iterable<DlqEnvelope> envelopes) {
            DlqEnvelope e = envelopes.iterator().next();
            Assertions.assertEquals("ValidateEventFn", e.getPipelineStage(),
                "Missing customer_id must route to DLQ with stage=ValidateEventFn");
            Assertions.assertEquals("ERR_MISSING_CUST", e.getErrorCode());
            Assertions.assertEquals("test-orders", e.getKafkaTopic());
            Assertions.assertNotNull(e.getDlqKey());
            return null;
          }
        });

    // Merged DLQ must have exactly 2 records (one parse failure + one validate failure)
    PAssert.that(mergedDlq).satisfies(
        new org.apache.beam.sdk.transforms.SerializableFunction<Iterable<DlqEnvelope>, Void>() {
          @Override
          public Void apply(Iterable<DlqEnvelope> envelopes) {
            int count = 0;
            for (DlqEnvelope ignored : envelopes) count++;
            Assertions.assertEquals(2, count,
                "Merged DLQ must contain exactly 2 records (1 parse + 1 validate failure)");
            return null;
          }
        });

    pipeline.run().waitUntilFinish();
  }
}
