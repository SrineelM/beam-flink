package com.enterprise.beam.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OrderUpdatedEvent extends BaseEvent {

  @JsonProperty("status")
  private String status;

  public OrderUpdatedEvent() {
    setEventType(EventType.ORDER_UPDATED);
  }

  public String getStatus() { return status; }
  public void setStatus(String status) { this.status = status; }
}
