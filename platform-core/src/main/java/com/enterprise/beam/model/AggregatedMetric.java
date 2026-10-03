package com.enterprise.beam.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Result model for windowed metric aggregations.
 */
public class AggregatedMetric implements Serializable {

  private static final long serialVersionUID = 1L;

  @JsonProperty("window_start")
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
  private Instant windowStart;

  @JsonProperty("window_end")
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
  private Instant windowEnd;

  @JsonProperty("window_type")
  private String windowType;

  @JsonProperty("region")
  private String region;

  @JsonProperty("event_type")
  private String eventType;

  @JsonProperty("gross_revenue")
  private BigDecimal grossRevenue = BigDecimal.ZERO;

  @JsonProperty("cancelled_revenue")
  private BigDecimal cancelledRevenue = BigDecimal.ZERO;

  @JsonProperty("total_revenue")
  private BigDecimal totalRevenue = BigDecimal.ZERO;

  @JsonProperty("order_count")
  private long orderCount;

  @JsonProperty("avg_order_value")
  private BigDecimal avgOrderValue = BigDecimal.ZERO;

  @JsonProperty("failed_payment_count")
  private long failedPaymentCount;

  @JsonProperty("failed_payment_rate")
  private double failedPaymentRate;

  @JsonProperty("cancellation_count")
  private long cancellationCount;

  @JsonProperty("unique_customers")
  private long uniqueCustomers;

  @JsonProperty("pane_index")
  private int paneIndex;

  public AggregatedMetric() {
  }

  public Instant getWindowStart() { return windowStart; }
  public void setWindowStart(Instant windowStart) { this.windowStart = windowStart; }

  public Instant getWindowEnd() { return windowEnd; }
  public void setWindowEnd(Instant windowEnd) { this.windowEnd = windowEnd; }

  public String getWindowType() { return windowType; }
  public void setWindowType(String windowType) { this.windowType = windowType; }

  public String getRegion() { return region; }
  public void setRegion(String region) { this.region = region; }

  public String getEventType() { return eventType; }
  public void setEventType(String eventType) { this.eventType = eventType; }

  public BigDecimal getGrossRevenue() { return grossRevenue; }
  public void setGrossRevenue(BigDecimal grossRevenue) { this.grossRevenue = grossRevenue; }

  public BigDecimal getCancelledRevenue() { return cancelledRevenue; }
  public void setCancelledRevenue(BigDecimal cancelledRevenue) { this.cancelledRevenue = cancelledRevenue; }

  public BigDecimal getTotalRevenue() { return totalRevenue; }
  public void setTotalRevenue(BigDecimal totalRevenue) { this.totalRevenue = totalRevenue; }

  public long getOrderCount() { return orderCount; }
  public void setOrderCount(long orderCount) { this.orderCount = orderCount; }

  public BigDecimal getAvgOrderValue() { return avgOrderValue; }
  public void setAvgOrderValue(BigDecimal avgOrderValue) { this.avgOrderValue = avgOrderValue; }

  public long getFailedPaymentCount() { return failedPaymentCount; }
  public void setFailedPaymentCount(long failedPaymentCount) { this.failedPaymentCount = failedPaymentCount; }

  public double getFailedPaymentRate() { return failedPaymentRate; }
  public void setFailedPaymentRate(double failedPaymentRate) { this.failedPaymentRate = failedPaymentRate; }

  public long getCancellationCount() { return cancellationCount; }
  public void setCancellationCount(long cancellationCount) { this.cancellationCount = cancellationCount; }

  public long getUniqueCustomers() { return uniqueCustomers; }
  public void setUniqueCustomers(long uniqueCustomers) { this.uniqueCustomers = uniqueCustomers; }

  public int getPaneIndex() { return paneIndex; }
  public void setPaneIndex(int paneIndex) { this.paneIndex = paneIndex; }
}
