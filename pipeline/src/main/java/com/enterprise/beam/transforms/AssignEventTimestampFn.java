package com.enterprise.beam.transforms;

import com.enterprise.beam.model.BaseEvent;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;

/**
 * DoFn for assigning event timestamps derived from BaseEvent payload fields.
 */
public class AssignEventTimestampFn extends DoFn<BaseEvent, BaseEvent> {
  private static final long serialVersionUID = 1L;

  @ProcessElement
  public void processElement(
      @Element BaseEvent event,
      OutputReceiver<BaseEvent> out,
      @Timestamp Instant currentTs) {
    if (event.getEventTime() != null) {
      Instant target = new Instant(event.getEventTime().toEpochMilli());
      if (target.isEqual(currentTs)) {
        out.output(event);
      } else {
        out.outputWithTimestamp(event, target);
      }
    } else {
      out.output(event);
    }
  }
}
