package com.enterprise.beam.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProductRemovedFromCartEvent extends BaseEvent {

  @JsonProperty("quantity")
  private int quantity;

  public ProductRemovedFromCartEvent() {
    setEventType(EventType.PRODUCT_REMOVED_FROM_CART);
  }

  public int getQuantity() { return quantity; }
  public void setQuantity(int quantity) { this.quantity = quantity; }
}
