package com.enterprise.beam.sinks;

import com.enterprise.beam.config.PipelineConfig;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;

import java.io.Serializable;

/**
 * Wrapper for writing JSON output streams to local directory files or cloud object storage.
 * Re-windows input streams to 1-minute fixed windows with processing-time trigger before writing.
 */
public class ObjectStorageSinkWrapper implements Serializable {

  private static final long serialVersionUID = 1L;

  private final PipelineConfig config;

  public ObjectStorageSinkWrapper(PipelineConfig config) {
    this.config = config;
  }

  public PTransform<PCollection<String>, PDone> writeToStorage(String prefix, String suffix) {
    String outputPrefix;
    if (config.isUseLocalObjectStorage()) {
      outputPrefix = config.getLocalObjectStoragePath() + "/" + prefix;
    } else {
      outputPrefix = "s3://" + config.getObjectStorageBucket() + "/" + prefix;
    }

    return new PTransform<PCollection<String>, PDone>() {
      private static final long serialVersionUID = 1L;

      @Override
      public PDone expand(PCollection<String> input) {
        return input
            .apply("ReWindowForStorage", Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
                .triggering(Repeatedly.forever(
                    AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(30))))
                .withAllowedLateness(Duration.ZERO)
                .discardingFiredPanes())
            .apply("WindowedTextIOWrite", TextIO.write()
                .to(outputPrefix)
                .withSuffix(suffix)
                .withWindowedWrites()
                .withNumShards(1));
      }
    };
  }
}
