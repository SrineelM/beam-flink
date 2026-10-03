package com.enterprise.beam.transforms;

import com.enterprise.beam.config.PipelineConfig;
import com.enterprise.beam.model.BaseEvent;
import com.enterprise.beam.model.CustomerViewedProductEvent;
import com.enterprise.beam.model.OrderCreatedEvent;
import com.enterprise.beam.model.PaymentFailedEvent;
import com.enterprise.beam.model.RiskSignal;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
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

public class CustomerRiskDetectorFnTest implements Serializable {

  private static final long serialVersionUID = 1L;

  @Test
  public void testThreeFailuresWithinTenMinutesEmitOnceAndFourthDoesNotReEmit() {
    Pipeline pipeline = Pipeline.create();
    String customerId = "CUST-FRAUD-1";
    Instant t0 = new Instant(1_700_000_000_000L);

    PaymentFailedEvent f1 = createFailure(customerId, t0);
    PaymentFailedEvent f2 = createFailure(customerId, t0.plus(Duration.standardMinutes(2)));
    PaymentFailedEvent f3 = createFailure(customerId, t0.plus(Duration.standardMinutes(5)));
    PaymentFailedEvent f4 = createFailure(customerId, t0.plus(Duration.standardMinutes(7)));
    CustomerViewedProductEvent view = new CustomerViewedProductEvent();
    view.setEventId(UUID.randomUUID());
    view.setCustomerId(customerId);
    view.setProductId("PROD-1");
    view.setEventTime(java.time.Instant.ofEpochMilli(t0.plus(Duration.standardMinutes(8)).getMillis()));

    TestStream<KV<String, BaseEvent>> stream = TestStream.create(
        KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(BaseEvent.class)))
        .advanceWatermarkTo(t0)
        .addElements(TimestampedValue.of(KV.of(customerId, (BaseEvent) f1), t0))
        .advanceWatermarkTo(t0.plus(Duration.standardMinutes(2)))
        .addElements(TimestampedValue.of(KV.of(customerId, (BaseEvent) f2), t0.plus(Duration.standardMinutes(2))))
        .advanceWatermarkTo(t0.plus(Duration.standardMinutes(5)))
        .addElements(TimestampedValue.of(KV.of(customerId, (BaseEvent) f3), t0.plus(Duration.standardMinutes(5))))
        .advanceWatermarkTo(t0.plus(Duration.standardMinutes(7)))
        .addElements(TimestampedValue.of(KV.of(customerId, (BaseEvent) f4), t0.plus(Duration.standardMinutes(7))))
        .advanceWatermarkTo(t0.plus(Duration.standardMinutes(8)))
        .addElements(TimestampedValue.of(KV.of(customerId, (BaseEvent) view), t0.plus(Duration.standardMinutes(8))))
        .advanceWatermarkToInfinity();

    PCollection<RiskSignal> signals = pipeline.apply(stream)
        .apply(ParDo.of(new CustomerRiskDetectorFn()));

    PAssert.that(signals).satisfies(results -> {
      List<RiskSignal> list = new ArrayList<>();
      results.forEach(list::add);
      Assertions.assertEquals(1, list.size(), "3 failures within 10 min must emit exactly once; 4th failure must not re-emit");
      RiskSignal signal = list.get(0);
      Assertions.assertEquals(customerId, signal.getCustomerId());
      Assertions.assertEquals(0.5, signal.getRiskScore(), 0.001);
      Assertions.assertEquals(3, signal.getPaymentFailureCount());
      Assertions.assertFalse(signal.isEscalated());
      Assertions.assertEquals("FRAUD_RISK", signal.getSignalType());
      Assertions.assertNotNull(signal.getSignalKey());
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testFailuresSpreadBeyondLookbackDoNotEmit() {
    Pipeline pipeline = Pipeline.create();
    String customerId = "CUST-SPREAD-1";
    Instant t0 = new Instant(1_700_000_000_000L);

    PaymentFailedEvent f1 = createFailure(customerId, t0);
    PaymentFailedEvent f2 = createFailure(customerId, t0.plus(Duration.standardMinutes(65))); // f1 expired
    PaymentFailedEvent f3 = createFailure(customerId, t0.plus(Duration.standardMinutes(130))); // f2 expired

    TestStream<KV<String, BaseEvent>> stream = TestStream.create(
        KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(BaseEvent.class)))
        .advanceWatermarkTo(t0)
        .addElements(TimestampedValue.of(KV.of(customerId, (BaseEvent) f1), t0))
        .advanceWatermarkTo(t0.plus(Duration.standardMinutes(65)))
        .addElements(TimestampedValue.of(KV.of(customerId, (BaseEvent) f2), t0.plus(Duration.standardMinutes(65))))
        .advanceWatermarkTo(t0.plus(Duration.standardMinutes(130)))
        .addElements(TimestampedValue.of(KV.of(customerId, (BaseEvent) f3), t0.plus(Duration.standardMinutes(130))))
        .advanceWatermarkToInfinity();

    PCollection<RiskSignal> signals = pipeline.apply(stream)
        .apply(ParDo.of(new CustomerRiskDetectorFn()));

    PAssert.that(signals).empty();
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testTenOrdersAndThreeFailuresEmitEscalated() {
    Pipeline pipeline = Pipeline.create();
    String customerId = "CUST-ESCALATED-1";
    Instant t0 = new Instant(1_700_000_000_000L);

    List<TimestampedValue<KV<String, BaseEvent>>> elements = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Instant t = t0.plus(Duration.standardMinutes(i));
      PaymentFailedEvent f = createFailure(customerId, t);
      elements.add(TimestampedValue.of(KV.of(customerId, (BaseEvent) f), t));
    }
    for (int i = 0; i < 10; i++) {
      Instant t = t0.plus(Duration.standardMinutes(5 + i));
      OrderCreatedEvent o = new OrderCreatedEvent();
      o.setEventId(UUID.randomUUID());
      o.setCustomerId(customerId);
      o.setAmount(new BigDecimal("20.00"));
      o.setEventTime(java.time.Instant.ofEpochMilli(t.getMillis()));
      elements.add(TimestampedValue.of(KV.of(customerId, (BaseEvent) o), t));
    }

    TestStream.Builder<KV<String, BaseEvent>> builder = TestStream.create(
        KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(BaseEvent.class)))
        .advanceWatermarkTo(t0);

    for (TimestampedValue<KV<String, BaseEvent>> tv : elements) {
      builder = builder.advanceWatermarkTo(tv.getTimestamp()).addElements(tv);
    }
    TestStream<KV<String, BaseEvent>> stream = builder
        .advanceWatermarkToInfinity();

    PCollection<RiskSignal> signals = pipeline.apply(stream)
        .apply(ParDo.of(new CustomerRiskDetectorFn()));

    PAssert.that(signals).satisfies(results -> {
      List<RiskSignal> list = new ArrayList<>();
      results.forEach(list::add);
      Assertions.assertEquals(2, list.size(), "Expected 2 signals: one at threshold (0.5), one escalated (0.8)");

      RiskSignal first = list.get(0);
      Assertions.assertEquals(0.5, first.getRiskScore(), 0.001);
      Assertions.assertFalse(first.isEscalated());

      RiskSignal second = list.get(1);
      Assertions.assertEquals(0.8, second.getRiskScore(), 0.001);
      Assertions.assertTrue(second.isEscalated());
      Assertions.assertEquals(10, second.getOrderAttemptCount());
      Assertions.assertEquals(new BigDecimal("200.00"), second.getTotalAmountLast1Hour());
      Assertions.assertNotNull(second.getSignalKey());
      Assertions.assertTrue(second.getSignalKey().contains("FRAUD_RISK|true|"));

      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  private PaymentFailedEvent createFailure(String customerId, Instant instant) {
    PaymentFailedEvent fail = new PaymentFailedEvent();
    fail.setEventId(UUID.randomUUID());
    fail.setCustomerId(customerId);
    fail.setEventTime(java.time.Instant.ofEpochMilli(instant.getMillis()));
    return fail;
  }

  /**
   * A3: A late event must land in its own minute bucket, not in the max-seen minute.
   * Scenario: 2 on-time failures (minute M), then 1 late failure (minute M-5) arrives
   * while watermark is at minute M+1. The late failure should count as minute M-5.
   * Total failures = 3 → signal emitted. Without the fix, the late event would be placed
   * in minute M+1, causing the signal_key to use the wrong minute.
   */
  @Test
  public void testLateEventLandsInItsOwnMinuteBucket() {
    Pipeline pipeline = Pipeline.create();
    String customerId = "CUST-LATE-BUCKET-1";
    Instant t0 = new Instant(1_700_000_000_000L);
    Instant tOnTime1 = t0;
    Instant tOnTime2 = t0.plus(Duration.standardMinutes(1));
    // Late event: timestamped 5 min before t0 but arrives late
    Instant tLate = t0.minus(Duration.standardMinutes(5));

    PaymentFailedEvent f1 = createFailure(customerId, tOnTime1);
    PaymentFailedEvent f2 = createFailure(customerId, tOnTime2);
    PaymentFailedEvent fLate = createFailure(customerId, tLate);

    TestStream<KV<String, BaseEvent>> stream = TestStream.create(
        KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(BaseEvent.class)))
        .advanceWatermarkTo(t0)
        .addElements(TimestampedValue.of(KV.of(customerId, (BaseEvent) f1), tOnTime1))
        .advanceWatermarkTo(tOnTime2)
        .addElements(TimestampedValue.of(KV.of(customerId, (BaseEvent) f2), tOnTime2))
        // Late event arrives while watermark is at t0+1min; its own timestamp is t0-5min
        .addElements(TimestampedValue.of(KV.of(customerId, (BaseEvent) fLate), tLate))
        .advanceWatermarkToInfinity();

    PCollection<RiskSignal> signals = pipeline.apply(stream)
        .apply(ParDo.of(new CustomerRiskDetectorFn()));

    PAssert.that(signals).satisfies(results -> {
      List<RiskSignal> list = new ArrayList<>();
      results.forEach(list::add);
      // 3 failures within lookback → signal emitted
      Assertions.assertFalse(list.isEmpty(),
          "A3: 3 failures within lookback must emit a signal");
      // signal_key must use the event's own minute, not the max-seen minute
      boolean allKeysCorrect = list.stream()
          .allMatch(s -> s.getSignalKey() != null && s.getSignalKey().contains("FRAUD_RISK"));
      Assertions.assertTrue(allKeysCorrect, "A3: signal_key must contain FRAUD_RISK");
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  /**
   * A3: A late event older than maxMinute - lookbackMinutes must be ignored for counting.
   * Scenario: max seen minute is M, lookback = 60 min. A late event at minute M-61 must
   * not increment any bucket and must not cause a signal to be emitted.
   */
  @Test
  public void testLateEventOlderThanLookbackIsNotCounted() {
    Pipeline pipeline = Pipeline.create();
    String customerId = "CUST-TOO-LATE-1";
    Instant t0 = new Instant(1_700_000_000_000L);
    // 2 failures just below threshold (default threshold = 3)
    PaymentFailedEvent f1 = createFailure(customerId, t0);
    PaymentFailedEvent f2 = createFailure(customerId, t0.plus(Duration.standardMinutes(1)));
    // Very old event: 65 minutes behind max (61 > lookback=60); must be ignored
    PaymentFailedEvent fTooLate = createFailure(customerId,
        t0.minus(Duration.standardMinutes(65)));

    TestStream<KV<String, BaseEvent>> stream = TestStream.create(
        KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(BaseEvent.class)))
        .advanceWatermarkTo(t0)
        .addElements(TimestampedValue.of(KV.of(customerId, (BaseEvent) f1), t0))
        .advanceWatermarkTo(t0.plus(Duration.standardMinutes(1)))
        .addElements(TimestampedValue.of(KV.of(customerId, (BaseEvent) f2),
            t0.plus(Duration.standardMinutes(1))))
        // fTooLate arrives late; its own minute is 65 min behind max
        .addElements(TimestampedValue.of(KV.of(customerId, (BaseEvent) fTooLate),
            t0.minus(Duration.standardMinutes(65))))
        .advanceWatermarkToInfinity();

    PCollection<RiskSignal> signals = pipeline.apply(stream)
        .apply(ParDo.of(new CustomerRiskDetectorFn()));

    // Only 2 counted failures (< threshold 3) → no signal
    PAssert.that(signals).empty();
    pipeline.run().waitUntilFinish();
  }

  /**
   * A3: signal_key must use the event's own minute (eventMinute), not the max-seen minute.
   * Scenario: one failure at minute M, one at minute M+5. Signal emits on the 3rd failure
   * at minute M+3. The signal_key minute must be M+3, not M+5.
   * Without the fix, signal_key would use maxMinute = M+5 for all events.
   */
  @Test
  public void testSignalKeyUsesEventOwnMinute() {
    Pipeline pipeline = Pipeline.create();
    String customerId = "CUST-SIGKEY-1";
    // Align t0 to a round minute boundary for predictable minute arithmetic
    long roundMillis = 1_700_000_000_000L - (1_700_000_000_000L % 60_000L);
    Instant t0 = new Instant(roundMillis);
    // Three failures: t0, t0+3min, t0+5min
    PaymentFailedEvent f1 = createFailure(customerId, t0);
    PaymentFailedEvent f2 = createFailure(customerId, t0.plus(Duration.standardMinutes(3)));
    PaymentFailedEvent f3 = createFailure(customerId, t0.plus(Duration.standardMinutes(5)));

    TestStream<KV<String, BaseEvent>> stream = TestStream.create(
        KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(BaseEvent.class)))
        .advanceWatermarkTo(t0)
        .addElements(TimestampedValue.of(KV.of(customerId, (BaseEvent) f1), t0))
        .advanceWatermarkTo(t0.plus(Duration.standardMinutes(3)))
        .addElements(TimestampedValue.of(KV.of(customerId, (BaseEvent) f2),
            t0.plus(Duration.standardMinutes(3))))
        .advanceWatermarkTo(t0.plus(Duration.standardMinutes(5)))
        .addElements(TimestampedValue.of(KV.of(customerId, (BaseEvent) f3),
            t0.plus(Duration.standardMinutes(5))))
        .advanceWatermarkToInfinity();

    PCollection<RiskSignal> signals = pipeline.apply(stream)
        .apply(ParDo.of(new CustomerRiskDetectorFn()));

    // f3 triggers the signal (3rd failure). Its signal_key minute must be
    // t0+5min's minute (= roundMillis/60000 + 5), not some other minute.
    long expectedMinuteOfF3 = (roundMillis + Duration.standardMinutes(5).getMillis()) / 60_000L;

    PAssert.that(signals).satisfies(results -> {
      List<RiskSignal> list = new ArrayList<>();
      results.forEach(list::add);
      Assertions.assertFalse(list.isEmpty(), "A3: Must emit signal on 3rd failure");
      RiskSignal signal = list.get(0);
      Assertions.assertNotNull(signal.getSignalKey(), "A3: signal_key must not be null");
      // signal_key format: customerId|signalType|escalated|minute
      String[] parts = signal.getSignalKey().split("\\|");
      Assertions.assertEquals(4, parts.length, "A3: signal_key must have 4 parts");
      long keyMinute = Long.parseLong(parts[3]);
      Assertions.assertEquals(expectedMinuteOfF3, keyMinute,
          "A3: signal_key minute must match event's own minute, not max-seen minute");
      return null;
    });

    pipeline.run().waitUntilFinish();
  }
}
