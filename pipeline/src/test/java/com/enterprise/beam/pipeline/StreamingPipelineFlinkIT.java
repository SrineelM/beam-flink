package com.enterprise.beam.pipeline;

import com.enterprise.beam.config.PipelineConfig;
import com.enterprise.beam.model.DlqEnvelope;
import com.enterprise.beam.sinks.InMemoryPipelineSinks;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Flink integration test: runs the pipeline with FlinkRunner against an embedded
 * Flink mini-cluster (--flinkMaster=[local]).
 *
 * <p>Scope: verifies the pipeline graph is accepted by the Flink runner and that the
 * sample source path routes the malformed event to DLQ and emits no DLQ for valid events.
 *
 * <p>Run with: {@code mvn verify -Pintegration-test -pl pipeline -am}
 *
 * <p>PAssert is unreliable on FlinkRunner in streaming mode because the runner submits
 * the job but PAssert assertions run on the driver side. Therefore, this test uses the
 * bounded sample source ({@code --useSampleSource=true}) which is a batch-like bounded
 * pipeline, and writes DLQ output to an {@link InMemoryPipelineSinks} to assert results.
 *
 * <p>B3 compatibility note: The Beam FlinkRunner artifact {@code beam-runners-flink-2.2}
 * targets Flink 2.2.x, which matches the project's Flink 2.2.1 dependency declared in
 * the parent pom. No version mismatch. See docs/technology-versions.md for the full
 * compatibility matrix.
 */
public class StreamingPipelineFlinkIT implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Builds and runs the pipeline with FlinkRunner on an embedded local mini-cluster.
   * Asserts that the DLQ contains exactly one entry (the malformed sample event) and
   * that at least one valid event is NOT in the DLQ.
   */
  @Test
  public void testFlinkRunnerWithSampleSourceAndInMemorySinks() {
    FlinkPipelineOptions options = PipelineOptionsFactory
        .fromArgs(
            "--runner=FlinkRunner",
            "--flinkMaster=[local]",
            "--streaming=false",
            "--useSampleSource=true",
            "--parallelism=1")
        .withValidation()
        .as(FlinkPipelineOptions.class);
    options.setStreaming(false); // sample source is bounded; streaming=false required
    options.setParallelism(1);

    Pipeline pipeline = Pipeline.create(options);
    PipelineConfig config = new PipelineConfig();
    // Fail open: no real sinks needed
    config.setSinkFailOpen(true);

    InMemoryPipelineSinks sinks = new InMemoryPipelineSinks();
    StreamingPipeline.buildPipeline(pipeline, config, null, sinks);

    PCollection<DlqEnvelope> dlq = sinks.getDlqEnvelopes();

    // Assert: exactly one malformed event in DLQ (the "{\"invalid_json_payload" sample)
    PAssert.that(dlq).satisfies(envelopes -> {
      List<DlqEnvelope> list = new ArrayList<>();
      envelopes.forEach(list::add);
      Assertions.assertEquals(1, list.size(),
          "FlinkIT: Only the malformed sample event must be in DLQ; found: " + list.size());
      DlqEnvelope d = list.get(0);
      Assertions.assertTrue(
          d.getOriginalPayload() != null
              && d.getOriginalPayload().contains("invalid_json_payload"),
          "FlinkIT: DLQ entry must be the malformed sample event, got: "
              + d.getOriginalPayload());
      Assertions.assertEquals("ParseEventFn", d.getPipelineStage(),
          "FlinkIT: DLQ entry must have stage=ParseEventFn");
      return null;
    });

    // Run the pipeline. PipelineResult.waitUntilFinish() blocks until Flink job completes.
    pipeline.run().waitUntilFinish();
  }
}
