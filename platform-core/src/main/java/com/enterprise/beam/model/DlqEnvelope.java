package com.enterprise.beam.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

/**
 * Envelope for dead-letter queue (DLQ) records.
 *
 * <p>Contains original raw event payload and debugging metadata.
 */
public class DlqEnvelope implements Serializable {

  private static final long serialVersionUID = 1L;

  @JsonProperty("original_payload")
  private String originalPayload;

  @JsonProperty("error_category")
  private ErrorCategory errorCategory;

  @JsonProperty("error_code")
  private String errorCode;

  @JsonProperty("error_message")
  private String errorMessage;

  @JsonProperty("stack_trace")
  private String stackTrace;

  @JsonProperty("pipeline_stage")
  private String pipelineStage;

  @JsonProperty("processing_timestamp")
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
  private Instant processingTimestamp = Instant.now();

  @JsonProperty("correlation_id")
  private UUID correlationId;

  @JsonProperty("kafka_topic")
  private String kafkaTopic;

  @JsonProperty("kafka_partition")
  private int kafkaPartition;

  @JsonProperty("kafka_offset")
  private long kafkaOffset;

  @JsonProperty("dlq_key")
  private String dlqKey;

  public DlqEnvelope() {
  }

  public String getOriginalPayload() { return originalPayload; }
  public void setOriginalPayload(String originalPayload) { this.originalPayload = originalPayload; }

  public ErrorCategory getErrorCategory() { return errorCategory; }
  public void setErrorCategory(ErrorCategory errorCategory) { this.errorCategory = errorCategory; }

  public String getErrorCode() { return errorCode; }
  public void setErrorCode(String errorCode) { this.errorCode = errorCode; }

  public String getErrorMessage() { return errorMessage; }
  public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

  public String getStackTrace() { return stackTrace; }
  public void setStackTrace(String stackTrace) { this.stackTrace = stackTrace; }

  public String getPipelineStage() { return pipelineStage; }
  public void setPipelineStage(String pipelineStage) { this.pipelineStage = pipelineStage; }

  public Instant getProcessingTimestamp() { return processingTimestamp; }
  public void setProcessingTimestamp(Instant processingTimestamp) { this.processingTimestamp = processingTimestamp; }

  public UUID getCorrelationId() { return correlationId; }
  public void setCorrelationId(UUID correlationId) { this.correlationId = correlationId; }

  public String getKafkaTopic() { return kafkaTopic; }
  public void setKafkaTopic(String kafkaTopic) { this.kafkaTopic = kafkaTopic; }

  public int getKafkaPartition() { return kafkaPartition; }
  public void setKafkaPartition(int kafkaPartition) { this.kafkaPartition = kafkaPartition; }

  public long getKafkaOffset() { return kafkaOffset; }
  public void setKafkaOffset(long kafkaOffset) { this.kafkaOffset = kafkaOffset; }

  public String getDlqKey() { return dlqKey; }
  public void setDlqKey(String dlqKey) { this.dlqKey = dlqKey; }
}
