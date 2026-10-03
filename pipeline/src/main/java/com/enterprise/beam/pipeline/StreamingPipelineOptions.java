package com.enterprise.beam.pipeline;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Custom pipeline options for {@link StreamingPipeline}.
 */
public interface StreamingPipelineOptions extends PipelineOptions {

  @Description("Whether to use the bounded sample source instead of Kafka")
  @Default.Boolean(false)
  boolean getUseSampleSource();

  void setUseSampleSource(boolean value);
}
