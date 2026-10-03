package com.enterprise.beam.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Signal emitted by customer risk detection stateful DoFn.
 */
public class RiskSignal implements Serializable {

  private static final long serialVersionUID = 1L;

  @JsonProperty("customer_id")
  private String customerId;

  @JsonProperty("risk_score")
  private double riskScore;

  @JsonProperty("payment_failure_count")
  private int paymentFailureCount;

  @JsonProperty("order_attempt_count")
  private int orderAttemptCount;

  @JsonProperty("total_amount_last_1hr")
  private BigDecimal totalAmountLast1Hour = BigDecimal.ZERO;

  @JsonProperty("signal_timestamp")
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
  private Instant signalTimestamp = Instant.now();

  @JsonProperty("escalated")
  private boolean escalated;

  @JsonProperty("signal_type")
  private String signalType = "FRAUD_RISK";

  @JsonProperty("signal_key")
  private String signalKey;

  public RiskSignal() {
  }

  public String getCustomerId() { return customerId; }
  public void setCustomerId(String customerId) { this.customerId = customerId; }

  public double getRiskScore() { return riskScore; }
  public void setRiskScore(double riskScore) { this.riskScore = riskScore; }

  public int getPaymentFailureCount() { return paymentFailureCount; }
  public void setPaymentFailureCount(int paymentFailureCount) { this.paymentFailureCount = paymentFailureCount; }

  public int getOrderAttemptCount() { return orderAttemptCount; }
  public void setOrderAttemptCount(int orderAttemptCount) { this.orderAttemptCount = orderAttemptCount; }

  public BigDecimal getTotalAmountLast1Hour() { return totalAmountLast1Hour; }
  public void setTotalAmountLast1Hour(BigDecimal totalAmountLast1Hour) { this.totalAmountLast1Hour = totalAmountLast1Hour; }

  public Instant getSignalTimestamp() { return signalTimestamp; }
  public void setSignalTimestamp(Instant signalTimestamp) { this.signalTimestamp = signalTimestamp; }

  public boolean isEscalated() { return escalated; }
  public void setEscalated(boolean escalated) { this.escalated = escalated; }

  public String getSignalType() { return signalType; }
  public void setSignalType(String signalType) { this.signalType = signalType; }

  public String getSignalKey() { return signalKey; }
  public void setSignalKey(String signalKey) { this.signalKey = signalKey; }
}
