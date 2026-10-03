package com.enterprise.beam.sinks;

import com.enterprise.beam.model.AggregatedMetric;
import com.enterprise.beam.model.DlqEnvelope;
import com.enterprise.beam.model.RiskSignal;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;

/**
 * Interface abstracting sink outputs for the streaming pipeline DAG.
 * Decouples pipeline assembly from physical I/O connectors for testing.
 */
public interface PipelineSinks extends Serializable {

  void sinkDlqToPostgres(PCollection<DlqEnvelope> dlq);

  void sinkDlqToKafka(PCollection<KV<String, String>> dlq);

  void sinkDlqToObjectStorage(PCollection<String> dlq);

  void sinkMetricsToPostgres(PCollection<AggregatedMetric> metrics);

  void sinkMetricsToKafka(PCollection<KV<String, String>> metrics);

  void sinkMetricsToObjectStorage(PCollection<String> metrics);

  void sinkRiskSignalsToPostgres(PCollection<RiskSignal> riskSignals);

  void sinkRiskSignalsToKafka(PCollection<KV<String, String>> riskSignals);

  void sinkCartAbandonmentToPostgres(PCollection<RiskSignal> cartSignals);

  void sinkCartAbandonmentToKafka(PCollection<KV<String, String>> cartSignals);
}
