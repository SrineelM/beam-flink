package com.enterprise.beam.transforms;

import com.enterprise.beam.model.BaseEvent;
import com.enterprise.beam.model.DlqEnvelope;
import com.enterprise.beam.model.OrderCreatedEvent;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
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
import java.util.UUID;

/**
 * Unit tests for late data handling and window pane semantics.
 *
 * <p>T1.1: Proves the 5-min tumbling window with {@code accumulatingFiredPanes()} produces:
 * <ol>
 *   <li>pane-0 (on-time): 1 element (the element that arrived before watermark crossed window end)
 *   <li>pane-1 (late): accumulated count=2 (on-time element + late element)
 * </ol>
 * These tests use a simple 2-stage pipeline (TestStream → Window → Assert) with NO
 * intermediate DoFn stages, so the watermark propagates immediately from source to window.
 * The full E2E pipeline cannot guarantee pane separation due to multi-stage watermark lag
 * (documented in docs/DESIGN.md "Known Behaviours").
 *
 * <p>T1.9: Proves late data older than {@code MAX_EVENT_AGE_DAYS} is routed to DLQ by
 * {@link ValidateEventFn} before it reaches any windowing stage.
 */
public class LateDataWindowingTest implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Helper: build a minimal {@link OrderCreatedEvent} for testing.
   *
   * @param eventId UUID string for the event
   * @param epochMilli event_time in epoch milliseconds
   * @param region region code (e.g. "US")
   */
  private static BaseEvent makeOrder(String eventId, long epochMilli, String region) {
    OrderCreatedEvent event = new OrderCreatedEvent();
    event.setEventId(UUID.fromString(eventId));
    event.setCustomerId("CUST-" + region);
    event.setRegion(region);
    event.setAmount(BigDecimal.valueOf(100));
    event.setEventTime(java.time.Instant.ofEpochMilli(epochMilli));
    return event;
  }

  /**
   * T1.1: Verifies that a 5-min window with {@code accumulatingFiredPanes()} produces two
   * separate panes when an on-time element and a late element are in the same window.
   *
   * <p>This test uses a DIRECT {@code TestStream<BaseEvent>} (no intermediate stages) to
   * ensure the watermark reaches the windowing stage immediately after each advance.
   */
  @Test
  public void testFiveMinWindowProducesTwoPanes() {
    Pipeline pipeline = Pipeline.create();

    // Use a fixed past timestamp that is far enough in the past that it is NOT affected
    // by ValidateEventFn's freshness check (this test bypasses validation entirely).
    // Window: [t0, t0+5min)
    long t0millis = 1_700_000_000_000L - (1_700_000_000_000L % 300_000L);
    Instant t0 = new Instant(t0millis);
    Instant tEvent1 = t0.plus(Duration.standardMinutes(1));
    Instant tEventLate = t0.plus(Duration.standardMinutes(2));
    Instant tWindowEnd = t0.plus(Duration.standardMinutes(5));

    BaseEvent order1 = makeOrder("00000000-0000-0000-0000-000000000001", tEvent1.getMillis(), "US");
    BaseEvent orderLate = makeOrder("00000000-0000-0000-0000-000000000002",
        tEventLate.getMillis(), "US");

    // Direct TestStream<BaseEvent>: 2-stage pipeline (no parse/validate/dedup stages).
    // This ensures the watermark advances synchronously with the window stage.
    TestStream<BaseEvent> stream = TestStream.create(SerializableCoder.of(BaseEvent.class))
        .advanceWatermarkTo(t0)
        .addElements(TimestampedValue.of(order1, tEvent1))
        // Advance watermark past window end: on-time pane fires with 1 element.
        .advanceWatermarkTo(tWindowEnd.plus(Duration.standardSeconds(1)))
        .advanceProcessingTime(Duration.standardSeconds(5))
        // Add late element (within allowedLateness=10min): late pane fires accumulating to 2.
        .addElements(TimestampedValue.of(orderLate, tEventLate))
        .advanceProcessingTime(Duration.standardSeconds(5))
        // Advance watermark past allowed-lateness boundary: ensures the late trigger fires.
        .advanceWatermarkTo(tWindowEnd.plus(Duration.standardMinutes(11)))
        .advanceProcessingTime(Duration.standardSeconds(10))
        .advanceWatermarkToInfinity();

    PCollection<BaseEvent> events = pipeline.apply("Source", stream);

    // Apply the same windowing strategy used in production (10-min allowed lateness).
    // Use Combine.perKey (keyed by region) to produce per-pane AggregatedMetric output.
    // This mirrors the production pipeline and uses the same path as the already-proven
    // RevenueAggregateCombineFnTest.testTumbling5MinLateDataAccumulatingPanePerRegion.
    PCollection<com.enterprise.beam.model.AggregatedMetric> metrics = events
        .apply("Apply5MinWindow",
            WindowingStrategy.<BaseEvent>tumbling5Min(Duration.standardMinutes(10)))
        .apply("KeyByRegion",
            org.apache.beam.sdk.transforms.WithKeys.of(
                (BaseEvent e) -> e.getRegion() != null ? e.getRegion() : "UNKNOWN")
                .withKeyType(org.apache.beam.sdk.values.TypeDescriptors.strings()))
        .apply("Combine", Combine.perKey(new RevenueAggregateCombineFn()))
        .apply("Stamp", ParDo.of(
            new DoFn<KV<String, com.enterprise.beam.model.AggregatedMetric>,
                com.enterprise.beam.model.AggregatedMetric>() {
              private static final long serialVersionUID = 1L;

              @ProcessElement
              public void processElement(
                  @Element KV<String, com.enterprise.beam.model.AggregatedMetric> kv,
                  OutputReceiver<com.enterprise.beam.model.AggregatedMetric> out,
                  org.apache.beam.sdk.transforms.windowing.BoundedWindow w,
                  org.apache.beam.sdk.transforms.windowing.PaneInfo paneInfo) {
                com.enterprise.beam.model.AggregatedMetric m = new com.enterprise.beam.model.AggregatedMetric();
                m.setOrderCount(kv.getValue().getOrderCount());
                m.setGrossRevenue(kv.getValue().getGrossRevenue());
                m.setRegion(kv.getKey());
                m.setPaneIndex((int) paneInfo.getIndex());
                out.output(m);
              }
            }));

    PAssert.that(metrics).satisfies(panes -> {
      List<com.enterprise.beam.model.AggregatedMetric> list = new ArrayList<>();
      panes.forEach(list::add);
      list.sort(Comparator.comparingInt(com.enterprise.beam.model.AggregatedMetric::getPaneIndex));

      Assertions.assertEquals(2, list.size(),
          "Expected 2 distinct panes (on-time pane-0 and late pane-1), but got: " + list.size());

      com.enterprise.beam.model.AggregatedMetric pane0 = list.get(0);
      Assertions.assertEquals(0, pane0.getPaneIndex(), "First pane must be pane-0 (on-time)");
      Assertions.assertEquals(1L, pane0.getOrderCount(),
          "On-time pane-0 must have orderCount=1 (only order1, before watermark passed window end)");
      Assertions.assertEquals(new java.math.BigDecimal("100"), pane0.getGrossRevenue());

      com.enterprise.beam.model.AggregatedMetric pane1 = list.get(1);
      Assertions.assertEquals(1, pane1.getPaneIndex(), "Second pane must be pane-1 (late)");
      Assertions.assertEquals(2L, pane1.getOrderCount(),
          "Late pane-1 must have accumulated orderCount=2 (order1 + orderLate, accumulatingFiredPanes)");
      Assertions.assertEquals(new java.math.BigDecimal("200"), pane1.getGrossRevenue(),
          "Late pane-1 must accumulate grossRevenue = 100 + 100 = 200");

      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  /**
   * T1.9: Proves that {@link ValidateEventFn} routes events older than the default
   * {@code MAX_EVENT_AGE_DAYS=7} to the DLQ with code {@code ERR_EVENT_TIME_OUT_OF_RANGE}.
   */
  @Test
  public void testStaleEventRoutedToDlqByValidateEventFn() {
    Pipeline pipeline = Pipeline.create();

    // 8-day-old event: beyond the default 7-day limit
    long eightDaysAgoMillis = System.currentTimeMillis() - 8L * 24 * 60 * 60 * 1000;
    BaseEvent staleEvent = makeOrder("aaaaaaaa-0000-0000-0000-000000000001",
        eightDaysAgoMillis, "US");

    PCollection<BaseEvent> input = pipeline.apply("StaleInput", Create.of(staleEvent));

    // Use default ValidateEventFn (maxEventAgeDays = 7) — 8-day-old event must fail
    PCollectionTuple result = input.apply("Validate",
        ParDo.of(new ValidateEventFn())
            .withOutputTags(ValidateEventFn.VALID_TAG,
                TupleTagList.of(ValidateEventFn.DLQ_TAG)));

    PAssert.that(result.get(ValidateEventFn.VALID_TAG)).empty();

    PAssert.that(result.get(ValidateEventFn.DLQ_TAG)).satisfies(envelopes -> {
      List<DlqEnvelope> list = new ArrayList<>();
      envelopes.forEach(list::add);
      Assertions.assertEquals(1, list.size(),
          "Expected exactly 1 DLQ entry for the 8-day-old stale event");
      Assertions.assertEquals("ERR_EVENT_TIME_OUT_OF_RANGE", list.get(0).getErrorCode(),
          "Stale event must be routed to DLQ with ERR_EVENT_TIME_OUT_OF_RANGE");
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  /**
   * T1.9: Proves {@code MAX_EVENT_AGE_DAYS} is configurable. An event older than
   * the default 7 days but within a custom 30-day limit is accepted (routes to valid output).
   */
  @Test
  public void testConfigurableMaxEventAgeDaysAcceptsSlightlyOlderEvent() {
    Pipeline pipeline = Pipeline.create();

    // 15-day-old event: fails default (7-day) but passes custom (30-day) limit
    long fifteenDaysAgoMillis = System.currentTimeMillis() - 15L * 24 * 60 * 60 * 1000;
    BaseEvent event15DaysOld = makeOrder("bbbbbbbb-0000-0000-0000-000000000001",
        fifteenDaysAgoMillis, "US");

    PCollection<BaseEvent> input = pipeline.apply("Input15DayOld", Create.of(event15DaysOld));

    // Use custom ValidateEventFn with maxEventAgeDays=30
    PCollectionTuple result = input.apply("ValidateCustom30Days",
        ParDo.of(new ValidateEventFn(30))  // 30-day limit accepts the 15-day-old event
            .withOutputTags(ValidateEventFn.VALID_TAG,
                TupleTagList.of(ValidateEventFn.DLQ_TAG)));

    // With 30-day limit, the 15-day-old event must pass validation (not go to DLQ)
    PAssert.that(result.get(ValidateEventFn.DLQ_TAG)).empty();
    PAssert.that(result.get(ValidateEventFn.VALID_TAG)).satisfies(events -> {
      int count = 0;
      for (BaseEvent ignored : events) count++;
      Assertions.assertEquals(1, count,
          "Event within 30-day custom limit must pass validation (1 valid event expected)");
      return null;
    });

    pipeline.run().waitUntilFinish();
  }
}
