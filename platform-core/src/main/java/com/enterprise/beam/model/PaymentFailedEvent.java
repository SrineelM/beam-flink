package com.enterprise.beam.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PaymentFailedEvent extends BaseEvent {

  @JsonProperty("failure_code")
  private String failureCode;

  @JsonProperty("failure_reason")
  private String failureReason;

  public PaymentFailedEvent() {
    setEventType(EventType.PAYMENT_FAILED);
  }

  public String getFailureCode() { return failureCode; }
  public void setFailureCode(String failureCode) { this.failureCode = failureCode; }

  public String getFailureReason() { return failureReason; }
  public void setFailureReason(String failureReason) { this.failureReason = failureReason; }
}
