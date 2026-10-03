package com.enterprise.beam.sinks;

import com.enterprise.beam.config.PipelineConfig;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Verifies that ObjectStorageSinkWrapper writes windowed files using TestStream.
 */
public class ObjectStorageSinkWrapperTest implements Serializable {

  private static final long serialVersionUID = 1L;

  @Test
  public void testWindowedDlqFileOutput(@TempDir Path tempDir) throws Exception {
    Pipeline pipeline = Pipeline.create();

    PipelineConfig config = new PipelineConfig();
    config.setUseLocalObjectStorage(true);
    config.setLocalObjectStoragePath(tempDir.toAbsolutePath().toString());

    TestStream<String> stream = TestStream.create(StringUtf8Coder.of())
        .advanceWatermarkTo(new Instant(0))
        .addElements("{\"error\":\"malformed_json_1\"}")
        .advanceProcessingTime(Duration.standardSeconds(35))
        .advanceWatermarkTo(new Instant(65000))
        .advanceProcessingTime(Duration.standardSeconds(35))
        .advanceWatermarkToInfinity();

    PCollection<String> input = pipeline.apply("StreamSource", stream);
    input.apply("WriteToStorage", new ObjectStorageSinkWrapper(config).writeToStorage("dlq/events", ".json"));

    pipeline.run().waitUntilFinish();

    try (Stream<Path> paths = Files.walk(tempDir)) {
      List<Path> jsonFiles = paths
          .filter(Files::isRegularFile)
          .filter(p -> p.toString().endsWith(".json"))
          .collect(Collectors.toList());

      Assertions.assertFalse(jsonFiles.isEmpty(), "At least one DLQ file should have been written to the temp dir");
      boolean hasContent = jsonFiles.stream().anyMatch(p -> {
        try {
          return Files.size(p) > 0;
        } catch (Exception e) {
          return false;
        }
      });
      Assertions.assertTrue(hasContent, "Written DLQ file should not be empty");

      // T2.4: Verify filenames contain expected year range (not year 290308 from Beam
      // TIMESTAMP_MAX_VALUE used as window boundary in some buggy configurations).
      boolean hasStaleTimestamp = jsonFiles.stream()
          .anyMatch(p -> p.getFileName().toString().contains("290308"));
      Assertions.assertFalse(hasStaleTimestamp,
          "T2.4: Output filenames must not contain year 290308 (Beam TIMESTAMP_MAX_VALUE). "
              + "Files: " + jsonFiles);
    }
  }
}
