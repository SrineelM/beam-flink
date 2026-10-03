package com.enterprise.beam.transforms;

import com.enterprise.beam.model.AggregatedMetric;
import com.enterprise.beam.model.BaseEvent;
import com.enterprise.beam.model.OrderCancelledEvent;
import com.enterprise.beam.model.OrderCreatedEvent;
import com.enterprise.beam.model.PaymentFailedEvent;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
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

public class RevenueAggregateCombineFnTest implements Serializable {

  private static final long serialVersionUID = 1L;

  @Test
  public void testRevenueCombineFnAccumulation() {
    RevenueAggregateCombineFn combineFn = new RevenueAggregateCombineFn();
    RevenueAggregateCombineFn.Accumulator accum = combineFn.createAccumulator();

    OrderCreatedEvent order1 = new OrderCreatedEvent();
    order1.setEventId(UUID.randomUUID());
    order1.setCustomerId("CUST-1");
    order1.setAmount(new BigDecimal("100.00"));

    OrderCreatedEvent order2 = new OrderCreatedEvent();
    order2.setEventId(UUID.randomUUID());
    order2.setCustomerId("CUST-2");
    order2.setAmount(new BigDecimal("200.00"));

    PaymentFailedEvent failed = new PaymentFailedEvent();
    failed.setEventId(UUID.randomUUID());
    failed.setCustomerId("CUST-1");

    accum = combineFn.addInput(accum, order1);
    accum = combineFn.addInput(accum, order2);
    accum = combineFn.addInput(accum, failed);

    AggregatedMetric result = combineFn.extractOutput(accum);

    Assertions.assertNotNull(result);
    Assertions.assertEquals(new BigDecimal("300.00"), result.getTotalRevenue());
    Assertions.assertEquals(2L, result.getOrderCount());
    Assertions.assertEquals(1L, result.getFailedPaymentCount());
    Assertions.assertEquals(2L, result.getUniqueCustomers());
    Assertions.assertEquals(new BigDecimal("150.00"), result.getAvgOrderValue());
  }

  @Test
  public void testCancellationCountAndRevenueDeduction() {
    RevenueAggregateCombineFn combineFn = new RevenueAggregateCombineFn();
    RevenueAggregateCombineFn.Accumulator accum = combineFn.createAccumulator();

    OrderCreatedEvent order = new OrderCreatedEvent();
    order.setEventId(UUID.randomUUID());
    order.setCustomerId("CUST-1");
    order.setAmount(new BigDecimal("100.00"));

    OrderCancelledEvent cancelled1 = new OrderCancelledEvent();
    cancelled1.setEventId(UUID.randomUUID());
    cancelled1.setCustomerId("CUST-CANCEL-1");
    cancelled1.setCancellationReason("OUT_OF_STOCK");
    cancelled1.setAmount(new BigDecimal("30.00"));

    OrderCancelledEvent cancelled2 = new OrderCancelledEvent();
    cancelled2.setEventId(UUID.randomUUID());
    cancelled2.setCustomerId("CUST-CANCEL-2");
    cancelled2.setCancellationReason("CHANGED_MIND");

    accum = combineFn.addInput(accum, order);
    accum = combineFn.addInput(accum, cancelled1);
    accum = combineFn.addInput(accum, cancelled2);

    AggregatedMetric result = combineFn.extractOutput(accum);

    Assertions.assertNotNull(result);
    Assertions.assertEquals(2L, result.getCancellationCount());
    Assertions.assertEquals(1L, result.getOrderCount());
    Assertions.assertEquals(new BigDecimal("100.00"), result.getGrossRevenue());
    Assertions.assertEquals(new BigDecimal("30.00"), result.getCancelledRevenue());
    Assertions.assertEquals(new BigDecimal("70.00"), result.getTotalRevenue());
  }

  /**
   * T1.4: Verifies avgOrderValue = grossRevenue / orderCount (not totalRevenue / orderCount).
   *
   * <p>When a window contains both orders and cancellations, using totalRevenue (net) could yield
   * a negative or misleadingly low average. Gross revenue is the correct basis for per-order
   * average value since cancellations may refer to prior-window orders.
   */
  @Test
  public void testAvgOrderValueUsesGrossRevenue() {
    RevenueAggregateCombineFn combineFn = new RevenueAggregateCombineFn();
    RevenueAggregateCombineFn.Accumulator accum = combineFn.createAccumulator();

    OrderCreatedEvent order1 = new OrderCreatedEvent();
    order1.setEventId(UUID.randomUUID());
    order1.setCustomerId("CUST-1");
    order1.setAmount(new BigDecimal("100.00"));

    OrderCreatedEvent order2 = new OrderCreatedEvent();
    order2.setEventId(UUID.randomUUID());
    order2.setCustomerId("CUST-2");
    order2.setAmount(new BigDecimal("200.00"));

    // A cancellation in the same window: totalRevenue = 300 - 80 = 220.
    // avgOrderValue must use grossRevenue(300) / orderCount(2) = 150.00, NOT 220/2 = 110.00.
    OrderCancelledEvent cancellation = new OrderCancelledEvent();
    cancellation.setEventId(UUID.randomUUID());
    cancellation.setCustomerId("CUST-3");
    cancellation.setCancellationReason("CHANGED_MIND");
    cancellation.setAmount(new BigDecimal("80.00"));

    accum = combineFn.addInput(accum, order1);
    accum = combineFn.addInput(accum, order2);
    accum = combineFn.addInput(accum, cancellation);

    AggregatedMetric result = combineFn.extractOutput(accum);

    // T1.4: avgOrderValue must use grossRevenue (300), NOT totalRevenue (220)
    Assertions.assertEquals(new BigDecimal("150.00"), result.getAvgOrderValue(),
        "avgOrderValue must use grossRevenue/orderCount = 300/2 = 150.00, not totalRevenue/orderCount");
    // Verify other fields to ensure correctness
    Assertions.assertEquals(new BigDecimal("300.00"), result.getGrossRevenue());
    Assertions.assertEquals(new BigDecimal("80.00"), result.getCancelledRevenue());
    Assertions.assertEquals(new BigDecimal("220.00"), result.getTotalRevenue());
    Assertions.assertEquals(2L, result.getOrderCount());
  }

  @Test
  public void testTumbling5MinLateDataAccumulatingPanePerRegion() {
    Pipeline pipeline = Pipeline.create();
    long windowStartMillis = 1_699_999_800_000L; // 5-minute aligned (divisible by 300_000)
    Instant tWindowStart = new Instant(windowStartMillis);
    Instant tEvent1 = tWindowStart.plus(Duration.standardMinutes(1));
    Instant tWindowEnd = tWindowStart.plus(Duration.standardMinutes(5));
    Instant tLateEvent = tWindowStart.plus(Duration.standardMinutes(2));

    OrderCreatedEvent event1 = new OrderCreatedEvent();
    event1.setEventId(UUID.randomUUID());
    event1.setCustomerId("CUST-EU-1");
    event1.setRegion("EU");
    event1.setAmount(new BigDecimal("100.00"));
    event1.setEventTime(java.time.Instant.ofEpochMilli(tEvent1.getMillis()));

    OrderCreatedEvent lateEvent = new OrderCreatedEvent();
    lateEvent.setEventId(UUID.randomUUID());
    lateEvent.setCustomerId("CUST-EU-2");
    lateEvent.setRegion("EU");
    lateEvent.setAmount(new BigDecimal("50.00"));
    lateEvent.setEventTime(java.time.Instant.ofEpochMilli(tLateEvent.getMillis()));

    TestStream<BaseEvent> stream = TestStream.create(SerializableCoder.of(BaseEvent.class))
        .advanceWatermarkTo(tEvent1)
        .addElements(TimestampedValue.of((BaseEvent) event1, tEvent1))
        .advanceWatermarkTo(tWindowEnd) // Triggers on-time pane
        .addElements(TimestampedValue.of((BaseEvent) lateEvent, tLateEvent)) // Late arrival within allowed lateness
        .advanceWatermarkTo(tWindowEnd.plus(Duration.standardSeconds(30))) // Triggers late pane
        .advanceWatermarkToInfinity();

    PCollection<AggregatedMetric> metrics = pipeline.apply(stream)
        .apply("ApplyTumbling5MinWindow",
            WindowingStrategy.<BaseEvent>tumbling5Min(Duration.standardMinutes(2)))
        .apply("KeyByRegion",
            WithKeys.of((BaseEvent e) -> e.getRegion() != null ? e.getRegion() : "UNKNOWN")
                .withKeyType(TypeDescriptors.strings()))
        .apply("CombineRevenueMetricsPerRegion",
            Combine.perKey(new RevenueAggregateCombineFn()))
        .apply("StampWindowMetadata", ParDo.of(new DoFn<KV<String, AggregatedMetric>, AggregatedMetric>() {
          private static final long serialVersionUID = 1L;
          @ProcessElement
          public void processElement(
              @Element KV<String, AggregatedMetric> kv,
              OutputReceiver<AggregatedMetric> out,
              BoundedWindow window,
              PaneInfo paneInfo) {
            AggregatedMetric m = kv.getValue();
            AggregatedMetric stamped = new AggregatedMetric();
            stamped.setGrossRevenue(m.getGrossRevenue());
            stamped.setCancelledRevenue(m.getCancelledRevenue());
            stamped.setTotalRevenue(m.getTotalRevenue());
            stamped.setOrderCount(m.getOrderCount());
            stamped.setRegion(kv.getKey());
            stamped.setPaneIndex((int) paneInfo.getIndex());
            if (window instanceof IntervalWindow) {
              IntervalWindow iw = (IntervalWindow) window;
              stamped.setWindowStart(java.time.Instant.ofEpochMilli(iw.start().getMillis()));
              stamped.setWindowEnd(java.time.Instant.ofEpochMilli(iw.end().getMillis()));
            }
            out.output(stamped);
          }
        }));

    PAssert.that(metrics).satisfies(panes -> {
      List<AggregatedMetric> list = new ArrayList<>();
      panes.forEach(list::add);
      list.sort(Comparator.comparingInt(AggregatedMetric::getPaneIndex));

      Assertions.assertEquals(2, list.size(), "Expected 2 panes (on-time and late)");

      AggregatedMetric onTimePane = list.get(0);
      Assertions.assertEquals(0, onTimePane.getPaneIndex());
      Assertions.assertEquals("EU", onTimePane.getRegion());
      Assertions.assertEquals(1L, onTimePane.getOrderCount());
      Assertions.assertEquals(new BigDecimal("100.00"), onTimePane.getTotalRevenue());
      Assertions.assertEquals(java.time.Instant.ofEpochMilli(windowStartMillis), onTimePane.getWindowStart());

      AggregatedMetric latePane = list.get(1);
      Assertions.assertEquals(1, latePane.getPaneIndex());
      Assertions.assertEquals("EU", latePane.getRegion());
      Assertions.assertEquals(2L, latePane.getOrderCount(), "Late pane must accumulate order count");
      Assertions.assertEquals(new BigDecimal("150.00"), latePane.getTotalRevenue(), "Late pane must accumulate total revenue");
      Assertions.assertEquals(onTimePane.getWindowStart(), latePane.getWindowStart(), "Window start must be identical across panes");
      Assertions.assertEquals(onTimePane.getWindowEnd(), latePane.getWindowEnd(), "Window end must be identical across panes");

      return null;
    });

    pipeline.run().waitUntilFinish();
  }
}
