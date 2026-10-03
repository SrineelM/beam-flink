package com.enterprise.beam.transforms;

import com.enterprise.beam.model.BaseEvent;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;

/**
 * Stateful DoFn that deduplicates events keyed by event_id using a ValueState&lt;Boolean&gt;
 * and a PROCESSING_TIME timer for TTL expiry.
 *
 * <p>On first sight of an event_id key, records seen=true and registers a PROCESSING_TIME timer
 * for now + TTL. Any duplicates arriving within the TTL window are silently dropped.
 * When the timer fires, the state is cleared, allowing subsequent events with the same key to pass.
 */
public class DeduplicateEventsFn extends DoFn<KV<String, BaseEvent>, BaseEvent> {

  private static final long serialVersionUID = 1L;

  @StateId("seen")
  private final StateSpec<ValueState<Boolean>> seenSpec = StateSpecs.value();

  @TimerId("expiry")
  private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  private final Duration ttl;

  public DeduplicateEventsFn(Duration ttl) {
    this.ttl = ttl != null ? ttl : Duration.standardDays(1);
  }

  public DeduplicateEventsFn(long ttlSeconds) {
    this(Duration.standardSeconds(ttlSeconds));
  }

  public DeduplicateEventsFn() {
    this(Duration.standardDays(1));
  }

  public Duration getTtl() {
    return ttl;
  }

  @ProcessElement
  public void processElement(
      @Element KV<String, BaseEvent> kv,
      OutputReceiver<BaseEvent> out,
      @StateId("seen") ValueState<Boolean> seenState,
      @TimerId("expiry") Timer expiryTimer) {

    Boolean isSeen = seenState.read();
    if (isSeen == null || !isSeen) {
      seenState.write(true);
      expiryTimer.offset(ttl).setRelative();
      out.output(kv.getValue());
    }
  }

  @OnTimer("expiry")
  public void onExpiry(
      @StateId("seen") ValueState<Boolean> seenState) {
    seenState.clear();
  }
}
