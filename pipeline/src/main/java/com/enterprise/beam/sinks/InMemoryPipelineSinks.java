package com.enterprise.beam.sinks;

import com.enterprise.beam.model.AggregatedMetric;
import com.enterprise.beam.model.DlqEnvelope;
import com.enterprise.beam.model.RiskSignal;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * In-memory test sinks capturing PCollections for assertions in unit and integration tests.
 */
public class InMemoryPipelineSinks implements PipelineSinks {

  private static final long serialVersionUID = 1L;

  private PCollection<DlqEnvelope> dlqEnvelopes;
  private PCollection<KV<String, String>> dlqKafka;
  private PCollection<String> dlqStorage;

  private PCollection<AggregatedMetric> metrics;
  private PCollection<KV<String, String>> metricsKafka;
  private PCollection<String> metricsStorage;

  private PCollection<RiskSignal> riskSignals;
  private PCollection<KV<String, String>> riskSignalsKafka;

  private PCollection<RiskSignal> cartAbandonmentSignals;
  private PCollection<KV<String, String>> cartAbandonmentKafka;

  @Override
  public void sinkDlqToPostgres(PCollection<DlqEnvelope> dlq) {
    this.dlqEnvelopes = dlq;
  }

  @Override
  public void sinkDlqToKafka(PCollection<KV<String, String>> dlq) {
    this.dlqKafka = dlq;
  }

  @Override
  public void sinkDlqToObjectStorage(PCollection<String> dlq) {
    this.dlqStorage = dlq;
  }

  @Override
  public void sinkMetricsToPostgres(PCollection<AggregatedMetric> metrics) {
    this.metrics = metrics;
  }

  @Override
  public void sinkMetricsToKafka(PCollection<KV<String, String>> metrics) {
    this.metricsKafka = metrics;
  }

  @Override
  public void sinkMetricsToObjectStorage(PCollection<String> metrics) {
    this.metricsStorage = metrics;
  }

  @Override
  public void sinkRiskSignalsToPostgres(PCollection<RiskSignal> riskSignals) {
    this.riskSignals = riskSignals;
  }

  @Override
  public void sinkRiskSignalsToKafka(PCollection<KV<String, String>> riskSignals) {
    this.riskSignalsKafka = riskSignals;
  }

  @Override
  public void sinkCartAbandonmentToPostgres(PCollection<RiskSignal> cartSignals) {
    this.cartAbandonmentSignals = cartSignals;
  }

  @Override
  public void sinkCartAbandonmentToKafka(PCollection<KV<String, String>> cartSignals) {
    this.cartAbandonmentKafka = cartSignals;
  }

  public PCollection<DlqEnvelope> getDlqEnvelopes() { return dlqEnvelopes; }
  public PCollection<KV<String, String>> getDlqKafka() { return dlqKafka; }
  public PCollection<String> getDlqStorage() { return dlqStorage; }

  public PCollection<AggregatedMetric> getMetrics() { return metrics; }
  public PCollection<KV<String, String>> getMetricsKafka() { return metricsKafka; }
  public PCollection<String> getMetricsStorage() { return metricsStorage; }

  public PCollection<RiskSignal> getRiskSignals() { return riskSignals; }
  public PCollection<KV<String, String>> getRiskSignalsKafka() { return riskSignalsKafka; }

  public PCollection<RiskSignal> getCartAbandonmentSignals() { return cartAbandonmentSignals; }
  public PCollection<KV<String, String>> getCartAbandonmentKafka() { return cartAbandonmentKafka; }
}
