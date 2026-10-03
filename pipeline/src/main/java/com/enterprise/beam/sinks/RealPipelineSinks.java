package com.enterprise.beam.sinks;

import com.enterprise.beam.config.PipelineConfig;
import com.enterprise.beam.model.AggregatedMetric;
import com.enterprise.beam.model.DlqEnvelope;
import com.enterprise.beam.model.RiskSignal;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Real production sinks writing to PostgreSQL, Kafka, and Object Storage.
 */
public class RealPipelineSinks implements PipelineSinks {

  private static final long serialVersionUID = 1L;

  private final PipelineConfig config;

  public RealPipelineSinks(PipelineConfig config) {
    this.config = config;
  }

  @Override
  public void sinkDlqToPostgres(PCollection<DlqEnvelope> dlq) {
    dlq.apply("SinkDLQToPostgres", ParDo.of(new PostgresSinkDoFn<>(config)));
  }

  @Override
  public void sinkDlqToKafka(PCollection<KV<String, String>> dlq) {
    dlq.apply("SinkDLQToKafka", ParDo.of(new KafkaSinkDoFn(config, config.getKafkaTopicDlq())));
  }

  @Override
  public void sinkDlqToObjectStorage(PCollection<String> dlq) {
    dlq.apply("SinkDLQToObjectStorage",
        new ObjectStorageSinkWrapper(config).writeToStorage("dlq/events", ".json"));
  }

  @Override
  public void sinkMetricsToPostgres(PCollection<AggregatedMetric> metrics) {
    metrics.apply("SinkMetricsToPostgres", ParDo.of(new PostgresSinkDoFn<>(config)));
  }

  @Override
  public void sinkMetricsToKafka(PCollection<KV<String, String>> metrics) {
    metrics.apply("SinkMetricsToKafka",
        ParDo.of(new KafkaSinkDoFn(config, config.getKafkaTopicMetricsOutput())));
  }

  @Override
  public void sinkMetricsToObjectStorage(PCollection<String> metrics) {
    metrics.apply("SinkMetricsToObjectStorage",
        new ObjectStorageSinkWrapper(config).writeToStorage("metrics/window", ".json"));
  }

  @Override
  public void sinkRiskSignalsToPostgres(PCollection<RiskSignal> riskSignals) {
    riskSignals.apply("SinkRiskSignalsToPostgres", ParDo.of(new PostgresSinkDoFn<>(config)));
  }

  @Override
  public void sinkRiskSignalsToKafka(PCollection<KV<String, String>> riskSignals) {
    riskSignals.apply("SinkRiskSignalsToKafka",
        ParDo.of(new KafkaSinkDoFn(config, config.getKafkaTopicAlerts())));
  }

  @Override
  public void sinkCartAbandonmentToPostgres(PCollection<RiskSignal> cartSignals) {
    cartSignals.apply("SinkCartAbandonmentToPostgres",
        ParDo.of(new PostgresSinkDoFn<>(config)));
  }

  @Override
  public void sinkCartAbandonmentToKafka(PCollection<KV<String, String>> cartSignals) {
    cartSignals.apply("SinkCartSignalsToKafka",
        ParDo.of(new KafkaSinkDoFn(config, config.getKafkaTopicCartAbandonment())));
  }
}
