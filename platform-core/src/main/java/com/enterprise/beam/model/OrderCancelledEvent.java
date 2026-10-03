package com.enterprise.beam.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OrderCancelledEvent extends BaseEvent {

  @JsonProperty("cancellation_reason")
  private String cancellationReason;

  public OrderCancelledEvent() {
    setEventType(EventType.ORDER_CANCELLED);
  }

  public String getCancellationReason() { return cancellationReason; }
  public void setCancellationReason(String cancellationReason) { this.cancellationReason = cancellationReason; }
}
