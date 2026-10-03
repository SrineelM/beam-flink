package com.enterprise.beam.transforms;

import com.enterprise.beam.model.BaseEvent;
import com.enterprise.beam.model.EventType;
import com.enterprise.beam.model.RiskSignal;
import com.enterprise.beam.util.IdempotencyKeyBuilder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;

import java.time.Instant;

/**
 * DoFn for detecting cart abandonment in session windows.
 *
 * <p>Receives {@code KV<String, Iterable<BaseEvent>>} per customer session window.
 * A session is considered abandoned when:
 * <ul>
 *   <li>Net cart additions (adds minus removals) is greater than 0</li>
 *   <li>There is no ORDER_CREATED event occurring after the last PRODUCT_ADDED_TO_CART event</li>
 * </ul>
 */
public class CartAbandonmentFn extends DoFn<KV<String, Iterable<BaseEvent>>, RiskSignal> {

  private static final long serialVersionUID = 1L;

  @ProcessElement
  public void processElement(
      @Element KV<String, Iterable<BaseEvent>> kv,
      OutputReceiver<RiskSignal> out,
      BoundedWindow window) {

    String customerId = kv.getKey();
    Iterable<BaseEvent> events = kv.getValue();
    if (customerId == null || events == null) {
      return;
    }

    long cartAdds = 0;
    long cartRemovals = 0;
    Instant lastCartAddTime = null;
    Instant orderTimeAfterLastCartAdd = null;

    // Scan events in the session
    for (BaseEvent e : events) {
      if (e == null || e.getEventType() == null) {
        continue;
      }
      Instant eventTime = e.getEventTime() != null
          ? e.getEventTime()
          : (window instanceof IntervalWindow
              ? Instant.ofEpochMilli(((IntervalWindow) window).start().getMillis())
              : Instant.EPOCH);

      if (e.getEventType() == EventType.PRODUCT_ADDED_TO_CART) {
        cartAdds++;
        if (lastCartAddTime == null || eventTime.isAfter(lastCartAddTime)) {
          lastCartAddTime = eventTime;
        }
      } else if (e.getEventType() == EventType.PRODUCT_REMOVED_FROM_CART) {
        cartRemovals++;
      }
    }

    // Check if any ORDER_CREATED occurred on or after the last cart add
    if (lastCartAddTime != null) {
      for (BaseEvent e : events) {
        if (e != null && e.getEventType() == EventType.ORDER_CREATED) {
          Instant eventTime = e.getEventTime() != null
              ? e.getEventTime()
              : (window instanceof IntervalWindow
                  ? Instant.ofEpochMilli(((IntervalWindow) window).start().getMillis())
                  : Instant.EPOCH);
          if (!eventTime.isBefore(lastCartAddTime)) {
            orderTimeAfterLastCartAdd = eventTime;
            break;
          }
        }
      }
    }

    // Abandoned only if net additions > 0 and no ORDER_CREATED after last add
    if (cartAdds - cartRemovals > 0 && lastCartAddTime != null && orderTimeAfterLastCartAdd == null) {
      RiskSignal signal = new RiskSignal();
      signal.setCustomerId(customerId);
      signal.setRiskScore(0.2);
      signal.setSignalTimestamp(Instant.ofEpochMilli(window.maxTimestamp().getMillis()));
      signal.setEscalated(false);
      signal.setSignalType("CART_ABANDONMENT");

      Instant windowStart = Instant.EPOCH;
      if (window instanceof IntervalWindow) {
        windowStart = Instant.ofEpochMilli(((IntervalWindow) window).start().getMillis());
      }
      String signalKey = IdempotencyKeyBuilder.buildCartAbandonmentKey(customerId, windowStart);
      signal.setSignalKey(signalKey);

      out.output(signal);
    }
  }
}
