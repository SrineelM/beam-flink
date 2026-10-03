package com.enterprise.beam.pipeline;

import com.enterprise.beam.config.PipelineConfig;
import com.enterprise.beam.model.DlqEnvelope;
import com.enterprise.beam.model.RiskSignal;
import com.enterprise.beam.sinks.InMemoryPipelineSinks;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for sample source quality (A2):
 * - Each sample JSON string is valid JSON.
 * - The 3 valid events are not in DLQ; the malformed one is.
 * - Object storage output file names do not contain stale date strings.
 */
public class SampleSourceTest implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * A2: Assert each valid sample JSON string in StreamingPipeline is parseable.
   * The strings are built dynamically, so we re-produce them here to verify the pattern.
   */
  @Test
  public void testSampleJsonStringsAreValidJson() {
    Instant now = Instant.now();
    String sampleTime1 = now.minusSeconds(120).toString();
    String sampleTime2 = now.minusSeconds(60).toString();
    String sampleTime3 = now.minusSeconds(30).toString();

    String[] validSamples = {
        "{\"event_id\":\"00000000-0000-0000-0000-000000000001\","
            + "\"event_type\":\"ORDER_CREATED\","
            + "\"customer_id\":\"c100\","
            + "\"amount\":150.00,"
            + "\"event_time\":\"" + sampleTime1 + "\"}",
        "{\"event_id\":\"00000000-0000-0000-0000-000000000002\","
            + "\"event_type\":\"PAYMENT_FAILED\","
            + "\"customer_id\":\"c101\","
            + "\"amount\":49.99,"
            + "\"event_time\":\"" + sampleTime2 + "\"}",
        "{\"event_id\":\"00000000-0000-0000-0000-000000000003\","
            + "\"event_type\":\"PRODUCT_ADDED_TO_CART\","
            + "\"customer_id\":\"c100\","
            + "\"product_id\":\"PROD-1\","
            + "\"amount\":25.00,"
            + "\"event_time\":\"" + sampleTime3 + "\"}"
    };

    for (String json : validSamples) {
      Assertions.assertDoesNotThrow(
          () -> MAPPER.readTree(json),
          "Sample JSON must be valid: " + json);
    }

    // The malformed sample should NOT parse
    String malformed = "{\"invalid_json_payload";
    Assertions.assertThrows(
        Exception.class,
        () -> MAPPER.readTree(malformed),
        "Malformed sample must not parse as JSON");
  }

  /**
   * A2: When --useSampleSource=true, the 3 valid events must not be in the DLQ
   * and the malformed one must be in the DLQ.
   */
  @Test
  public void testSampleSourceValidEventsNotInDlqMalformedInDlq() {
    StreamingPipelineOptions options =
        PipelineOptionsFactory.as(StreamingPipelineOptions.class);
    options.setUseSampleSource(true);

    Pipeline pipeline = Pipeline.create(options);
    PipelineConfig config = new PipelineConfig();
    InMemoryPipelineSinks sinks = new InMemoryPipelineSinks();
    StreamingPipeline.buildPipeline(pipeline, config, null, sinks);

    PCollection<DlqEnvelope> dlq = sinks.getDlqEnvelopes();

    // Exactly one malformed event must be in DLQ
    PAssert.that(dlq).satisfies(envelopes -> {
      List<DlqEnvelope> list = new ArrayList<>();
      envelopes.forEach(list::add);
      Assertions.assertEquals(1, list.size(),
          "Only the malformed sample event must be in DLQ; found: " + list.size());
      DlqEnvelope d = list.get(0);
      Assertions.assertTrue(
          d.getOriginalPayload() != null && d.getOriginalPayload().contains("invalid_json_payload"),
          "DLQ entry must be the malformed sample, got: " + d.getOriginalPayload());
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  /**
   * A2: DirectRunner run with sample source using a TempDir for object storage.
   * File names must not contain stale date strings like "290308" that appeared in
   * committed output before the sample timestamps were made relative to now().
   */
  @Test
  public void testSampleSourceObjectStorageFileNamesDoNotContainStaleDate(@TempDir Path tempDir) {
    StreamingPipelineOptions options =
        PipelineOptionsFactory.as(StreamingPipelineOptions.class);
    options.setUseSampleSource(true);

    Pipeline pipeline = Pipeline.create(options);
    PipelineConfig config = new PipelineConfig();
    config.setLocalObjectStoragePath(tempDir.toString());

    InMemoryPipelineSinks sinks = new InMemoryPipelineSinks();
    StreamingPipeline.buildPipeline(pipeline, config, null, sinks);
    pipeline.run().waitUntilFinish();

    // Verify no output files in the temp dir contain "290308" (stale date symptom)
    try {
      List<String> staleFiles = new ArrayList<>();
      if (tempDir.toFile().exists()) {
        for (File f : tempDir.toFile().listFiles() != null
            ? tempDir.toFile().listFiles() : new File[0]) {
          if (f.getName().contains("290308")) {
            staleFiles.add(f.getName());
          }
        }
      }
      Assertions.assertTrue(staleFiles.isEmpty(),
          "Output file names must not contain stale date '290308': " + staleFiles);
    } catch (Exception e) {
      Assertions.fail("Error checking output files: " + e.getMessage());
    }
  }
}
