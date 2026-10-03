package com.enterprise.beam.simulator;

import com.enterprise.beam.model.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;

/**
 * Factory for creating synthetic events for simulation testing.
 */
public class EventFactory {

  private final SimulatorConfig config;
  private final Random random;

  public EventFactory(SimulatorConfig config) {
    this.config = config;
    this.random = new Random(config.getSeed());
  }

  public BaseEvent createEvent(OrderStateMachine stateMachine) {
    String customerId = "cust-" + random.nextInt(config.getCustomerCount());
    String productId = "prod-" + random.nextInt(config.getProductCount());
    String[] regions = config.getRegionList();
    String region = regions[random.nextInt(regions.length)];
    String orderId = "ord-" + UUID.randomUUID().toString().substring(0, 8);

    OrderStateMachine.State currentState = stateMachine.getState(orderId);
    Instant now = Instant.now();

    // Check for late event simulation
    if (random.nextDouble() < config.getLateEventRate()) {
      long delaySeconds = random.nextInt(config.getMaxLateSeconds());
      now = now.minusSeconds(delaySeconds);
    }

    BaseEvent event;

    switch (currentState) {
      case NEW:
        OrderCreatedEvent created = new OrderCreatedEvent();
        created.setAmount(BigDecimal.valueOf(100 + random.nextDouble() * 900).setScale(2, RoundingMode.HALF_UP));
        created.setItemCount(1 + random.nextInt(5));
        created.setPaymentMethod(random.nextBoolean() ? "CREDIT_CARD" : "UPI");
        stateMachine.updateState(orderId, OrderStateMachine.State.CREATED);
        event = created;
        break;
      case CREATED:
        if (random.nextDouble() < config.getPaymentFailureRate()) {
          PaymentFailedEvent failed = new PaymentFailedEvent();
          failed.setFailureCode("ERR_INSUFFICIENT_FUNDS");
          failed.setFailureReason("Card declined by issuer");
          stateMachine.updateState(orderId, OrderStateMachine.State.PAYMENT_FAILED);
          event = failed;
        } else {
          PaymentAuthorizedEvent auth = new PaymentAuthorizedEvent();
          auth.setPaymentGateway("STRIPE");
          auth.setTransactionReference("tx-" + UUID.randomUUID().toString().substring(0, 8));
          stateMachine.updateState(orderId, OrderStateMachine.State.PAYMENT_AUTHORIZED);
          event = auth;
        }
        break;
      default:
        CustomerViewedProductEvent viewed = new CustomerViewedProductEvent();
        viewed.setDurationSeconds(5 + random.nextInt(120));
        event = viewed;
        break;
    }

    event.setEventId(UUID.randomUUID());
    event.setEventTime(now);
    event.setIngestionTime(Instant.now());
    event.setCorrelationId(UUID.randomUUID());
    event.setProducerId("simulator-1");
    event.setCustomerId(customerId);
    event.setOrderId(orderId);
    event.setProductId(productId);
    event.setRegion(region);

    return event;
  }

  public String generateMalformedJson() {
    return "{\"event_id\":\"" + UUID.randomUUID() + "\", \"event_type\": \"ORDER_CREATED\", \"amount\": INVALID_NUMBER}";
  }
}
