package com.enterprise.beam.model;

/**
 * Enumeration of all 10 domain event types supported by the platform.
 *
 * <p>Each event type maps to a specific Kafka topic and carries a schema version.
 */
public enum EventType {
  ORDER_CREATED("orders", "1.0"),
  ORDER_UPDATED("orders", "1.0"),
  PAYMENT_AUTHORIZED("payments", "1.0"),
  PAYMENT_FAILED("payments", "1.0"),
  ORDER_CANCELLED("orders", "1.0"),
  SHIPMENT_CREATED("orders", "1.0"),
  SHIPMENT_DELIVERED("orders", "1.0"),
  CUSTOMER_VIEWED_PRODUCT("customer-events", "1.0"),
  PRODUCT_ADDED_TO_CART("customer-events", "1.0"),
  PRODUCT_REMOVED_FROM_CART("customer-events", "1.0");

  private final String defaultTopic;
  private final String schemaVersion;

  EventType(String defaultTopic, String schemaVersion) {
    this.defaultTopic = defaultTopic;
    this.schemaVersion = schemaVersion;
  }

  public String getDefaultTopic() {
    return defaultTopic;
  }

  public String getSchemaVersion() {
    return schemaVersion;
  }
}
