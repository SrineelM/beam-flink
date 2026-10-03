package com.enterprise.beam.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ShipmentCreatedEvent extends BaseEvent {

  @JsonProperty("carrier")
  private String carrier;

  @JsonProperty("tracking_number")
  private String trackingNumber;

  public ShipmentCreatedEvent() {
    setEventType(EventType.SHIPMENT_CREATED);
  }

  public String getCarrier() { return carrier; }
  public void setCarrier(String carrier) { this.carrier = carrier; }

  public String getTrackingNumber() { return trackingNumber; }
  public void setTrackingNumber(String trackingNumber) { this.trackingNumber = trackingNumber; }
}
