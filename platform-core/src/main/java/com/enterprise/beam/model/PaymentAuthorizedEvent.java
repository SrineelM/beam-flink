package com.enterprise.beam.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PaymentAuthorizedEvent extends BaseEvent {

  @JsonProperty("payment_gateway")
  private String paymentGateway;

  @JsonProperty("transaction_reference")
  private String transactionReference;

  public PaymentAuthorizedEvent() {
    setEventType(EventType.PAYMENT_AUTHORIZED);
  }

  public String getPaymentGateway() { return paymentGateway; }
  public void setPaymentGateway(String paymentGateway) { this.paymentGateway = paymentGateway; }

  public String getTransactionReference() { return transactionReference; }
  public void setTransactionReference(String transactionReference) { this.transactionReference = transactionReference; }
}
