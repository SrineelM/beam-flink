package com.enterprise.beam.transforms;

import com.enterprise.beam.model.BaseEvent;
import com.enterprise.beam.model.OrderCreatedEvent;
import com.enterprise.beam.model.ProductAddedToCartEvent;
import com.enterprise.beam.model.ProductRemovedFromCartEvent;
import com.enterprise.beam.model.RiskSignal;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Unit and TestStream integration tests for {@link CartAbandonmentFn}.
 */
public class CartAbandonmentFnTest implements Serializable {

  private static final long serialVersionUID = 1L;

  @Test
  public void testCartAbandonmentDetected() {
    Pipeline pipeline = Pipeline.create();
    Instant baseTime = new Instant(1_700_000_000_000L);

    ProductAddedToCartEvent cartAdd = new ProductAddedToCartEvent();
    cartAdd.setEventId(UUID.randomUUID());
    cartAdd.setCustomerId("CUST-ABANDON-1");
    cartAdd.setProductId("PROD-X");
    cartAdd.setAmount(new BigDecimal("49.99"));
    cartAdd.setEventTime(java.time.Instant.ofEpochMilli(baseTime.getMillis()));

    TestStream<KV<String, BaseEvent>> stream = TestStream.create(
        KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(BaseEvent.class)))
        .advanceWatermarkTo(baseTime)
        .addElements(TimestampedValue.of(KV.of("CUST-ABANDON-1", (BaseEvent) cartAdd), baseTime))
        .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(35)))
        .advanceWatermarkToInfinity();

    PCollection<RiskSignal> signals = pipeline
        .apply(stream)
        .apply("SessionWindow30Min", WindowingStrategy.<KV<String, BaseEvent>>session30MinGap())
        .apply("GroupCustomerSessionEvents", GroupByKey.create())
        .apply("DetectAbandonment", ParDo.of(new CartAbandonmentFn()));

    PAssert.that(signals).satisfies(results -> {
      List<RiskSignal> list = new ArrayList<>();
      results.forEach(list::add);
      Assertions.assertEquals(1, list.size());
      RiskSignal signal = list.get(0);
      Assertions.assertEquals("CUST-ABANDON-1", signal.getCustomerId());
      Assertions.assertEquals(0.2, signal.getRiskScore(), 0.001);
      Assertions.assertEquals("CART_ABANDONMENT", signal.getSignalType());
      Assertions.assertNotNull(signal.getSignalKey());
      Assertions.assertTrue(signal.getSignalKey().startsWith("CUST-ABANDON-1|CART|"));
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testOrderAfterAddSuppressesAbandonment() {
    Pipeline pipeline = Pipeline.create();
    Instant baseTime = new Instant(1_700_000_000_000L);

    ProductAddedToCartEvent cartAdd = new ProductAddedToCartEvent();
    cartAdd.setEventId(UUID.randomUUID());
    cartAdd.setCustomerId("CUST-COMPLETE-1");
    cartAdd.setProductId("PROD-Y");
    cartAdd.setAmount(new BigDecimal("99.00"));
    cartAdd.setEventTime(java.time.Instant.ofEpochMilli(baseTime.getMillis()));

    Instant orderTime = baseTime.plus(Duration.standardMinutes(5));
    OrderCreatedEvent order = new OrderCreatedEvent();
    order.setEventId(UUID.randomUUID());
    order.setCustomerId("CUST-COMPLETE-1");
    order.setAmount(new BigDecimal("99.00"));
    order.setEventTime(java.time.Instant.ofEpochMilli(orderTime.getMillis()));

    TestStream<KV<String, BaseEvent>> stream = TestStream.create(
        KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(BaseEvent.class)))
        .advanceWatermarkTo(baseTime)
        .addElements(
            TimestampedValue.of(KV.of("CUST-COMPLETE-1", (BaseEvent) cartAdd), baseTime),
            TimestampedValue.of(KV.of("CUST-COMPLETE-1", (BaseEvent) order), orderTime))
        .advanceWatermarkTo(orderTime.plus(Duration.standardMinutes(35)))
        .advanceWatermarkToInfinity();

    PCollection<RiskSignal> signals = pipeline
        .apply(stream)
        .apply("SessionWindow30Min", WindowingStrategy.<KV<String, BaseEvent>>session30MinGap())
        .apply("GroupCustomerSessionEvents", GroupByKey.create())
        .apply("DetectAbandonment", ParDo.of(new CartAbandonmentFn()));

    PAssert.that(signals).empty();
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testAddThenRemoveSuppressesAbandonment() {
    Pipeline pipeline = Pipeline.create();
    Instant baseTime = new Instant(1_700_000_000_000L);

    ProductAddedToCartEvent cartAdd = new ProductAddedToCartEvent();
    cartAdd.setEventId(UUID.randomUUID());
    cartAdd.setCustomerId("CUST-REMOVE-1");
    cartAdd.setProductId("PROD-Z");
    cartAdd.setAmount(new BigDecimal("25.00"));
    cartAdd.setEventTime(java.time.Instant.ofEpochMilli(baseTime.getMillis()));

    Instant removeTime = baseTime.plus(Duration.standardMinutes(2));
    ProductRemovedFromCartEvent cartRemove = new ProductRemovedFromCartEvent();
    cartRemove.setEventId(UUID.randomUUID());
    cartRemove.setCustomerId("CUST-REMOVE-1");
    cartRemove.setQuantity(1);
    cartRemove.setEventTime(java.time.Instant.ofEpochMilli(removeTime.getMillis()));

    TestStream<KV<String, BaseEvent>> stream = TestStream.create(
        KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(BaseEvent.class)))
        .advanceWatermarkTo(baseTime)
        .addElements(
            TimestampedValue.of(KV.of("CUST-REMOVE-1", (BaseEvent) cartAdd), baseTime),
            TimestampedValue.of(KV.of("CUST-REMOVE-1", (BaseEvent) cartRemove), removeTime))
        .advanceWatermarkTo(removeTime.plus(Duration.standardMinutes(35)))
        .advanceWatermarkToInfinity();

    PCollection<RiskSignal> signals = pipeline
        .apply(stream)
        .apply("SessionWindow30Min", WindowingStrategy.<KV<String, BaseEvent>>session30MinGap())
        .apply("GroupCustomerSessionEvents", GroupByKey.create())
        .apply("DetectAbandonment", ParDo.of(new CartAbandonmentFn()));

    PAssert.that(signals).empty();
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testTwoSessionsSeparatedByGapYieldTwoSignals() {
    Pipeline pipeline = Pipeline.create();
    Instant session1Time = new Instant(1_700_000_000_000L);
    Instant session2Time = session1Time.plus(Duration.standardMinutes(40));

    ProductAddedToCartEvent add1 = new ProductAddedToCartEvent();
    add1.setEventId(UUID.randomUUID());
    add1.setCustomerId("CUST-TWO-SESSIONS");
    add1.setProductId("PROD-1");
    add1.setAmount(new BigDecimal("10.00"));
    add1.setEventTime(java.time.Instant.ofEpochMilli(session1Time.getMillis()));

    ProductAddedToCartEvent add2 = new ProductAddedToCartEvent();
    add2.setEventId(UUID.randomUUID());
    add2.setCustomerId("CUST-TWO-SESSIONS");
    add2.setProductId("PROD-2");
    add2.setAmount(new BigDecimal("20.00"));
    add2.setEventTime(java.time.Instant.ofEpochMilli(session2Time.getMillis()));

    TestStream<KV<String, BaseEvent>> stream = TestStream.create(
        KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(BaseEvent.class)))
        .advanceWatermarkTo(session1Time)
        .addElements(TimestampedValue.of(KV.of("CUST-TWO-SESSIONS", (BaseEvent) add1), session1Time))
        .advanceWatermarkTo(session2Time)
        .addElements(TimestampedValue.of(KV.of("CUST-TWO-SESSIONS", (BaseEvent) add2), session2Time))
        .advanceWatermarkTo(session2Time.plus(Duration.standardMinutes(35)))
        .advanceWatermarkToInfinity();

    PCollection<RiskSignal> signals = pipeline
        .apply(stream)
        .apply("SessionWindow30Min", WindowingStrategy.<KV<String, BaseEvent>>session30MinGap())
        .apply("GroupCustomerSessionEvents", GroupByKey.create())
        .apply("DetectAbandonment", ParDo.of(new CartAbandonmentFn()));

    PAssert.that(signals).satisfies(results -> {
      List<RiskSignal> list = new ArrayList<>();
      results.forEach(list::add);
      Assertions.assertEquals(2, list.size(), "Expected 2 abandonment signals for 2 separate sessions");
      Assertions.assertNotEquals(list.get(0).getSignalKey(), list.get(1).getSignalKey());
      return null;
    });

    pipeline.run().waitUntilFinish();
  }
}
