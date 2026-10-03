package com.enterprise.beam.simulator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * State machine for generating order lifecycle events.
 */
public class OrderStateMachine {

  public enum State {
    NEW,
    CREATED,
    PAYMENT_AUTHORIZED,
    PAYMENT_FAILED,
    CANCELLED,
    SHIPPED,
    DELIVERED
  }

  private final Map<String, State> orderStates = new ConcurrentHashMap<>();

  public State getState(String orderId) {
    return orderStates.getOrDefault(orderId, State.NEW);
  }

  public void updateState(String orderId, State nextState) {
    orderStates.put(orderId, nextState);
  }

  public void clearOrder(String orderId) {
    orderStates.remove(orderId);
  }
}
