package com.enterprise.beam.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ShipmentDeliveredEvent extends BaseEvent {

  @JsonProperty("delivery_status")
  private String deliveryStatus;

  public ShipmentDeliveredEvent() {
    setEventType(EventType.SHIPMENT_DELIVERED);
  }

  public String getDeliveryStatus() { return deliveryStatus; }
  public void setDeliveryStatus(String deliveryStatus) { this.deliveryStatus = deliveryStatus; }
}
