package com.enterprise.beam.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CustomerViewedProductEvent extends BaseEvent {

  @JsonProperty("duration_seconds")
  private int durationSeconds;

  public CustomerViewedProductEvent() {
    setEventType(EventType.CUSTOMER_VIEWED_PRODUCT);
  }

  public int getDurationSeconds() { return durationSeconds; }
  public void setDurationSeconds(int durationSeconds) { this.durationSeconds = durationSeconds; }
}
