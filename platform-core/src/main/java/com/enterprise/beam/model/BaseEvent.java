package com.enterprise.beam.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Abstract base class for all real-time platform events.
 *
 * <p>Uses Jackson polymorphic deserialization mapped to {@link EventType}.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "event_type",
    visible = true
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = OrderCreatedEvent.class, name = "ORDER_CREATED"),
    @JsonSubTypes.Type(value = OrderUpdatedEvent.class, name = "ORDER_UPDATED"),
    @JsonSubTypes.Type(value = PaymentAuthorizedEvent.class, name = "PAYMENT_AUTHORIZED"),
    @JsonSubTypes.Type(value = PaymentFailedEvent.class, name = "PAYMENT_FAILED"),
    @JsonSubTypes.Type(value = OrderCancelledEvent.class, name = "ORDER_CANCELLED"),
    @JsonSubTypes.Type(value = ShipmentCreatedEvent.class, name = "SHIPMENT_CREATED"),
    @JsonSubTypes.Type(value = ShipmentDeliveredEvent.class, name = "SHIPMENT_DELIVERED"),
    @JsonSubTypes.Type(value = CustomerViewedProductEvent.class, name = "CUSTOMER_VIEWED_PRODUCT"),
    @JsonSubTypes.Type(value = ProductAddedToCartEvent.class, name = "PRODUCT_ADDED_TO_CART"),
    @JsonSubTypes.Type(value = ProductRemovedFromCartEvent.class, name = "PRODUCT_REMOVED_FROM_CART")
})
public abstract class BaseEvent implements Serializable {

  private static final long serialVersionUID = 1L;

  @JsonProperty("event_id")
  private UUID eventId;

  @JsonProperty("event_type")
  private EventType eventType;

  @JsonProperty("event_version")
  private int eventVersion = 1;

  @JsonProperty("schema_version")
  private String schemaVersion = "1.0";

  @JsonProperty("event_time")
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  private Instant eventTime;

  @JsonProperty("ingestion_time")
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  private Instant ingestionTime;

  @JsonProperty("correlation_id")
  private UUID correlationId;

  @JsonProperty("producer_id")
  private String producerId;

  @JsonProperty("customer_id")
  private String customerId;

  @JsonProperty("order_id")
  private String orderId;

  @JsonProperty("product_id")
  private String productId;

  @JsonProperty("region")
  private String region;

  @JsonProperty("currency")
  private String currency = "INR";

  @JsonProperty("amount")
  private BigDecimal amount;

  @JsonProperty("metadata")
  private Map<String, String> metadata = new HashMap<>();

  @JsonProperty("kafka_topic")
  private String kafkaTopic;

  @JsonProperty("kafka_partition")
  private int kafkaPartition;

  @JsonProperty("kafka_offset")
  private long kafkaOffset;

  public BaseEvent() {
  }

  public UUID getEventId() { return eventId; }
  public void setEventId(UUID eventId) { this.eventId = eventId; }

  public EventType getEventType() { return eventType; }
  public void setEventType(EventType eventType) { this.eventType = eventType; }

  public int getEventVersion() { return eventVersion; }
  public void setEventVersion(int eventVersion) { this.eventVersion = eventVersion; }

  public String getSchemaVersion() { return schemaVersion; }
  public void setSchemaVersion(String schemaVersion) { this.schemaVersion = schemaVersion; }

  public Instant getEventTime() { return eventTime; }
  public void setEventTime(Instant eventTime) { this.eventTime = eventTime; }

  public Instant getIngestionTime() { return ingestionTime; }
  public void setIngestionTime(Instant ingestionTime) { this.ingestionTime = ingestionTime; }

  public UUID getCorrelationId() { return correlationId; }
  public void setCorrelationId(UUID correlationId) { this.correlationId = correlationId; }

  public String getProducerId() { return producerId; }
  public void setProducerId(String producerId) { this.producerId = producerId; }

  public String getCustomerId() { return customerId; }
  public void setCustomerId(String customerId) { this.customerId = customerId; }

  public String getOrderId() { return orderId; }
  public void setOrderId(String orderId) { this.orderId = orderId; }

  public String getProductId() { return productId; }
  public void setProductId(String productId) { this.productId = productId; }

  public String getRegion() { return region; }
  public void setRegion(String region) { this.region = region; }

  public String getCurrency() { return currency; }
  public void setCurrency(String currency) { this.currency = currency; }

  public BigDecimal getAmount() { return amount; }
  public void setAmount(BigDecimal amount) { this.amount = amount; }

  public Map<String, String> getMetadata() { return metadata; }
  public void setMetadata(Map<String, String> metadata) { this.metadata = metadata; }

  public String getKafkaTopic() { return kafkaTopic; }
  public void setKafkaTopic(String kafkaTopic) { this.kafkaTopic = kafkaTopic; }

  public int getKafkaPartition() { return kafkaPartition; }
  public void setKafkaPartition(int kafkaPartition) { this.kafkaPartition = kafkaPartition; }

  public long getKafkaOffset() { return kafkaOffset; }
  public void setKafkaOffset(long kafkaOffset) { this.kafkaOffset = kafkaOffset; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BaseEvent baseEvent = (BaseEvent) o;
    return Objects.equals(eventId, baseEvent.eventId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(eventId);
  }

  @Override
  public String toString() {
    return "BaseEvent{" +
        "eventId=" + eventId +
        ", eventType=" + eventType +
        ", eventTime=" + eventTime +
        ", customerId='" + customerId + '\'' +
        '}';
  }
}
