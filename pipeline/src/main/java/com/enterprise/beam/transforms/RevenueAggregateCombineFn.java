package com.enterprise.beam.transforms;

import com.enterprise.beam.model.AggregatedMetric;
import com.enterprise.beam.model.BaseEvent;
import com.enterprise.beam.model.OrderCancelledEvent;
import com.enterprise.beam.model.OrderCreatedEvent;
import com.enterprise.beam.model.PaymentFailedEvent;
import org.apache.beam.sdk.transforms.Combine;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashSet;
import java.util.Set;

/**
 * CombineFn for aggregating order and revenue business metrics across windows.
 *
 * <p>Note on avgOrderValue: {@code avgOrderValue = grossRevenue / orderCount}.
 * Gross revenue counts only ORDER_CREATED amounts. Cancellations in the same window
 * may refer to prior-window orders; using net revenue (gross - cancelled) would
 * produce a negative average in windows with more cancellations than new orders.
 *
 * <p>Note on unique customer tracking:
 * The {@link HashSet} accumulator used for tracking unique customer IDs provides exact cardinality,
 * but is unbounded in memory. For high-volume production scale, this should be replaced with
 * an approximate cardinality estimator such as HyperLogLog (see Open items in docs/DESIGN.md).
 */
public class RevenueAggregateCombineFn extends Combine.CombineFn<BaseEvent, RevenueAggregateCombineFn.Accumulator, AggregatedMetric> {

  private static final long serialVersionUID = 1L;

  public static class Accumulator implements Serializable {
    private static final long serialVersionUID = 1L;

    BigDecimal grossRevenue = BigDecimal.ZERO;
    BigDecimal cancelledRevenue = BigDecimal.ZERO;
    BigDecimal totalRevenue = BigDecimal.ZERO;
    long orderCount = 0;
    long failedPaymentCount = 0;
    long cancellationCount = 0;
    Set<String> uniqueCustomers = new HashSet<>();
  }

  @Override
  public Accumulator createAccumulator() {
    return new Accumulator();
  }

  @Override
  public Accumulator addInput(Accumulator accum, BaseEvent input) {
    if (input.getCustomerId() != null) {
      accum.uniqueCustomers.add(input.getCustomerId());
    }

    if (input instanceof OrderCreatedEvent) {
      accum.orderCount++;
      if (input.getAmount() != null) {
        accum.grossRevenue = accum.grossRevenue.add(input.getAmount());
      }
    } else if (input instanceof PaymentFailedEvent) {
      accum.failedPaymentCount++;
    } else if (input instanceof OrderCancelledEvent) {
      accum.cancellationCount++;
      if (input.getAmount() != null) {
        accum.cancelledRevenue = accum.cancelledRevenue.add(input.getAmount());
      }
    }

    accum.totalRevenue = accum.grossRevenue.subtract(accum.cancelledRevenue);
    return accum;
  }

  @Override
  public Accumulator mergeAccumulators(Iterable<Accumulator> accumulators) {
    Accumulator merged = createAccumulator();
    for (Accumulator acc : accumulators) {
      merged.grossRevenue = merged.grossRevenue.add(acc.grossRevenue);
      merged.cancelledRevenue = merged.cancelledRevenue.add(acc.cancelledRevenue);
      merged.orderCount += acc.orderCount;
      merged.failedPaymentCount += acc.failedPaymentCount;
      merged.cancellationCount += acc.cancellationCount;
      merged.uniqueCustomers.addAll(acc.uniqueCustomers);
    }
    merged.totalRevenue = merged.grossRevenue.subtract(merged.cancelledRevenue);
    return merged;
  }

  @Override
  public AggregatedMetric extractOutput(Accumulator accum) {
    AggregatedMetric metric = new AggregatedMetric();
    metric.setGrossRevenue(accum.grossRevenue);
    metric.setCancelledRevenue(accum.cancelledRevenue);
    metric.setTotalRevenue(accum.totalRevenue);
    metric.setOrderCount(accum.orderCount);
    metric.setFailedPaymentCount(accum.failedPaymentCount);
    metric.setCancellationCount(accum.cancellationCount);
    metric.setUniqueCustomers(accum.uniqueCustomers.size());

    if (accum.orderCount > 0) {
      metric.setAvgOrderValue(accum.grossRevenue.divide(BigDecimal.valueOf(accum.orderCount), 2, RoundingMode.HALF_UP));
    }

    long totalTx = accum.orderCount + accum.failedPaymentCount;
    if (totalTx > 0) {
      metric.setFailedPaymentRate((double) accum.failedPaymentCount / totalTx);
    }

    return metric;
  }
}
