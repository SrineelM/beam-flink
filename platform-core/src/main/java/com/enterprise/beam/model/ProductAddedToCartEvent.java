package com.enterprise.beam.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProductAddedToCartEvent extends BaseEvent {

  @JsonProperty("quantity")
  private int quantity;

  public ProductAddedToCartEvent() {
    setEventType(EventType.PRODUCT_ADDED_TO_CART);
  }

  public int getQuantity() { return quantity; }
  public void setQuantity(int quantity) { this.quantity = quantity; }
}
