package com.enterprise.beam.transforms;

import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

/**
 * Factory class for reusable Beam windowing strategies.
 */
public class WindowingStrategy {

  private WindowingStrategy() {
  }

  /**
   * 1-minute fixed (tumbling) window. For high-frequency short-burst KPIs.
   */
  public static <T> Window<T> tumbling1Min() {
    return Window.<T>into(FixedWindows.of(Duration.standardMinutes(1)))
        .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
        .withAllowedLateness(Duration.standardMinutes(10))
        .discardingFiredPanes();
  }

  /**
   * 5-minute fixed (tumbling) window with custom allowed lateness.
   * Uses AfterWatermark.pastEndOfWindow().withLateFirings(AfterPane.elementCountAtLeast(1)), accumulatingFiredPanes().
   */
  public static <T> Window<T> tumbling5Min(Duration allowedLateness) {
    return Window.<T>into(FixedWindows.of(Duration.standardMinutes(5)))
        .triggering(AfterWatermark.pastEndOfWindow()
            .withLateFirings(AfterPane.elementCountAtLeast(1)))
        .withAllowedLateness(allowedLateness)
        .accumulatingFiredPanes();
  }

  /**
   * 5-minute fixed (tumbling) window with default 10-minute allowed lateness.
   */
  public static <T> Window<T> tumbling5Min() {
    return tumbling5Min(Duration.standardMinutes(10));
  }

  /**
   * 5-minute sliding window with 1-minute slide (overlapping windows).
   */
  public static <T> Window<T> sliding5Min1MinSlide() {
    return Window.<T>into(SlidingWindows.of(Duration.standardMinutes(5)).every(Duration.standardMinutes(1)))
        .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
        .withAllowedLateness(Duration.standardMinutes(10))
        .discardingFiredPanes();
  }

  /**
   * 30-minute session gap window for cart abandonment detection.
   * Fires on watermark passing end-of-window only, zero allowed lateness, discarding fired panes.
   */
  public static <T> Window<T> session30MinGap() {
    return Window.<T>into(Sessions.withGapDuration(Duration.standardMinutes(30)))
        .triggering(AfterWatermark.pastEndOfWindow())
        .withAllowedLateness(Duration.ZERO)
        .discardingFiredPanes();
  }
}
