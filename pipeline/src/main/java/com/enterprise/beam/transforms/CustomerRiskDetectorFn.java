package com.enterprise.beam.transforms;

import com.enterprise.beam.config.PipelineConfig;
import com.enterprise.beam.model.BaseEvent;
import com.enterprise.beam.model.EventType;
import com.enterprise.beam.model.RiskSignal;
import com.enterprise.beam.util.IdempotencyKeyBuilder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OnTimer;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.DoFn.TimerId;
import org.apache.beam.sdk.transforms.DoFn.Timestamp;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Stateful DoFn for real-time customer fraud and risk detection.
 *
 * <p>Maintains per-minute bucketed counts of payment failures, order attempts, and order amount
 * over a sliding lookback window (default 60 minutes). Buckets older than the lookback window
 * are evicted on every incoming event. An event-time cleanup timer purges expired state when
 * the customer is inactive.
 *
 * <p>To prevent alert storms, a {@link ValueState} records the last emitted risk score; signals
 * are emitted only when the score increases or newly crosses the escalation threshold.
 */
public class CustomerRiskDetectorFn extends DoFn<KV<String, BaseEvent>, RiskSignal> {

  private static final long serialVersionUID = 1L;

  private final int lookbackMinutes;
  private final int failureThreshold;
  private final int velocityThreshold;
  private final double weightFailure;
  private final double weightVelocity;
  private final double escalateAt;
  private final double emitAt;

  @StateId("minuteBuckets")
  private final StateSpec<MapState<Long, long[]>> minuteBucketsSpec =
      StateSpecs.map(BigEndianLongCoder.of(), SerializableCoder.of(long[].class));

  @StateId("lastEmittedScore")
  private final StateSpec<ValueState<Double>> lastEmittedScoreSpec =
      StateSpecs.value(DoubleCoder.of());

  /** Tracks the maximum event-time millis seen so far for this customer key.
   * Used to prevent an out-of-order late event from shortening the cleanup timer. */
  @StateId("maxEventMillis")
  private final StateSpec<ValueState<Long>> maxEventMillisSpec =
      StateSpecs.value(BigEndianLongCoder.of());

  @TimerId("cleanupTimer")
  private final TimerSpec cleanupTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  public CustomerRiskDetectorFn() {
    this(new PipelineConfig());
  }

  public CustomerRiskDetectorFn(PipelineConfig config) {
    this.lookbackMinutes = config.getRiskLookbackMinutes();
    this.failureThreshold = config.getRiskFailureThreshold();
    this.velocityThreshold = config.getRiskVelocityThreshold();
    this.weightFailure = 0.5;
    this.weightVelocity = 0.3;
    this.escalateAt = config.getEscalateAt();
    this.emitAt = config.getEmitAt();
  }

  @ProcessElement
  public void processElement(
      @Element KV<String, BaseEvent> kv,
      OutputReceiver<RiskSignal> out,
      @StateId("minuteBuckets") MapState<Long, long[]> minuteBuckets,
      @StateId("lastEmittedScore") ValueState<Double> lastEmittedScoreState,
      @StateId("maxEventMillis") ValueState<Long> maxEventMillisState,
      @TimerId("cleanupTimer") Timer cleanupTimer,
      @Timestamp org.joda.time.Instant elementTimestamp) {

    BaseEvent event = kv.getValue();
    String customerId = kv.getKey();
    long elementMillis = elementTimestamp.getMillis();

    // Update the maximum event time seen for this customer key. This ensures the
    // cleanup timer is never moved backward by a late out-of-order event.
    Long prevMaxMillis = maxEventMillisState.read();
    long maxMillis = (prevMaxMillis == null || elementMillis > prevMaxMillis)
        ? elementMillis : prevMaxMillis;
    if (prevMaxMillis == null || elementMillis > prevMaxMillis) {
      maxEventMillisState.write(maxMillis);
    }

    // A3: Use max-seen minute ONLY for eviction cutoff and the cleanup timer.
    long maxMinute = maxMillis / 60_000L;
    long cutoffMinute = maxMinute - lookbackMinutes;

    // A3: The event's own minute is used for bucket placement and signal_key.
    long eventMinute = elementMillis / 60_000L;

    // A3: Ignore late events that fall outside the lookback window. They cannot
    // contribute to any live bucket. Update the cleanup timer and return.
    if (eventMinute <= cutoffMinute) {
      org.joda.time.Instant maxTimestamp = new org.joda.time.Instant(maxMillis);
      cleanupTimer.set(maxTimestamp.plus(Duration.standardMinutes(lookbackMinutes + 1)));
      return;
    }

    // 1. Evict expired buckets older than lookback and aggregate active buckets
    List<Long> expiredKeys = new ArrayList<>();
    long totalFailures = 0;
    long totalOrders = 0;
    long totalAmountMinor = 0;

    Iterable<Map.Entry<Long, long[]>> entries = minuteBuckets.entries().read();
    if (entries != null) {
      for (Map.Entry<Long, long[]> entry : entries) {
        long bucket = entry.getKey();
        if (bucket <= cutoffMinute) {
          expiredKeys.add(bucket);
        } else {
          long[] counts = entry.getValue();
          if (counts != null) {
            totalFailures += counts[0];
            totalOrders += counts[1];
            totalAmountMinor += counts[2];
          }
        }
      }
    }
    for (Long exp : expiredKeys) {
      minuteBuckets.remove(exp);
    }

    // 2. Add current event to the event's own minute bucket (A3: not maxMinute)
    long[] eventBucketCounts = minuteBuckets.get(eventMinute).read();
    if (eventBucketCounts == null) {
      eventBucketCounts = new long[3];
    }
    boolean bucketChanged = false;
    if (event.getEventType() == EventType.PAYMENT_FAILED) {
      eventBucketCounts[0]++;
      totalFailures++;
      bucketChanged = true;
    } else if (event.getEventType() == EventType.ORDER_CREATED) {
      eventBucketCounts[1]++;
      totalOrders++;
      if (event.getAmount() != null) {
        long amountMinor = event.getAmount().multiply(BigDecimal.valueOf(100)).longValue();
        eventBucketCounts[2] += amountMinor;
        totalAmountMinor += amountMinor;
      }
      bucketChanged = true;
    }

    if (bucketChanged) {
      minuteBuckets.put(eventMinute, eventBucketCounts);
    }

    // 3. Compute score
    double riskScore = 0.0;
    if (totalFailures >= failureThreshold) {
      riskScore += weightFailure;
    }
    if (totalOrders >= velocityThreshold) {
      riskScore += weightVelocity;
    }

    // 4. Emission logic
    Double prevScoreObj = lastEmittedScoreState.read();
    double prevScore = prevScoreObj != null ? prevScoreObj : 0.0;

    boolean shouldEmit = false;
    if (riskScore >= emitAt) {
      if (riskScore > prevScore) {
        shouldEmit = true;
      } else if (riskScore >= escalateAt && prevScore < escalateAt) {
        shouldEmit = true;
      }
    } else {
      if (prevScoreObj != null) {
        lastEmittedScoreState.clear();
      }
    }

    if (shouldEmit) {
      boolean escalated = riskScore >= escalateAt;
      lastEmittedScoreState.write(riskScore);

      RiskSignal signal = new RiskSignal();
      signal.setCustomerId(customerId);
      signal.setRiskScore(riskScore);
      signal.setPaymentFailureCount((int) totalFailures);
      signal.setOrderAttemptCount((int) totalOrders);
      signal.setTotalAmountLast1Hour(BigDecimal.valueOf(totalAmountMinor, 2));
      signal.setSignalTimestamp(Instant.ofEpochMilli(elementTimestamp.getMillis()));
      signal.setEscalated(escalated);
      signal.setSignalType("FRAUD_RISK");

      // A3: signal_key uses the event's own minute (eventMinute), not the max-seen minute.
      String signalKey = IdempotencyKeyBuilder.buildRiskSignalKey(
          customerId, "FRAUD_RISK", escalated, eventMinute);
      signal.setSignalKey(signalKey);

      out.output(signal);
    }

    // 5. Event-time cleanup timer: set from the maximum seen event time, not the current element.
    // This prevents a late out-of-order event from moving the timer backward and clearing state
    // prematurely. The timer fires lookbackMinutes + 1 minute after the latest observed event.
    org.joda.time.Instant maxTimestamp = new org.joda.time.Instant(maxMillis);
    cleanupTimer.set(maxTimestamp.plus(Duration.standardMinutes(lookbackMinutes + 1)));
  }

  @OnTimer("cleanupTimer")
  public void onCleanup(
      @StateId("minuteBuckets") MapState<Long, long[]> minuteBuckets,
      @StateId("lastEmittedScore") ValueState<Double> lastEmittedScoreState,
      @StateId("maxEventMillis") ValueState<Long> maxEventMillisState) {
    minuteBuckets.clear();
    lastEmittedScoreState.clear();
    maxEventMillisState.clear();
  }
}
