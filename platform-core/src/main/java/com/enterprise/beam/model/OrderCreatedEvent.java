package com.enterprise.beam.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;

public class OrderCreatedEvent extends BaseEvent {

  @JsonProperty("item_count")
  private int itemCount;

  @JsonProperty("payment_method")
  private String paymentMethod;

  public OrderCreatedEvent() {
    setEventType(EventType.ORDER_CREATED);
  }

  public int getItemCount() { return itemCount; }
  public void setItemCount(int itemCount) { this.itemCount = itemCount; }

  public String getPaymentMethod() { return paymentMethod; }
  public void setPaymentMethod(String paymentMethod) { this.paymentMethod = paymentMethod; }
}
