package com.enterprise.beam.transforms;

import com.enterprise.beam.model.BaseEvent;
import com.enterprise.beam.model.DlqEnvelope;
import com.enterprise.beam.model.ErrorCategory;
import com.enterprise.beam.model.ValidationResult;
import com.enterprise.beam.util.IdempotencyKeyBuilder;
import com.enterprise.beam.util.JsonUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

/**
 * DoFn for business rule validation and data quality checks.
 * Validates mandatory fields, timestamp freshness, and positive monetary amounts.
 *
 * <p>The maximum allowed event age is controlled by the {@code MAX_EVENT_AGE_DAYS}
 * environment variable (default: {@value #DEFAULT_MAX_EVENT_AGE_DAYS} days). Events
 * older than this value are routed to the DLQ with code {@code ERR_EVENT_TIME_OUT_OF_RANGE}.
 * Note: events that pass this check but are older than (watermark - allowedLateness) are
 * dropped silently by the windowing layer — see docs/DESIGN.md "Known behaviours".
 */
public class ValidateEventFn extends DoFn<BaseEvent, BaseEvent> {

  static final int DEFAULT_MAX_EVENT_AGE_DAYS = 7;

  private static final long serialVersionUID = 1L;

  public static final TupleTag<BaseEvent> VALID_TAG = new TupleTag<BaseEvent>() {};
  public static final TupleTag<DlqEnvelope> DLQ_TAG = new TupleTag<DlqEnvelope>() {};

  private final int maxEventAgeDays;

  /** Reads {@code MAX_EVENT_AGE_DAYS} from the environment; falls back to 7. */
  public ValidateEventFn() {
    this(readMaxEventAgeDays());
  }

  /** Constructor for tests that need a custom age limit. */
  public ValidateEventFn(int maxEventAgeDays) {
    this.maxEventAgeDays = maxEventAgeDays;
  }

  private static int readMaxEventAgeDays() {
    String val = System.getenv("MAX_EVENT_AGE_DAYS");
    if (val != null && !val.trim().isEmpty()) {
      try {
        return Integer.parseInt(val.trim());
      } catch (NumberFormatException ignored) {}
    }
    return DEFAULT_MAX_EVENT_AGE_DAYS;
  }

  @ProcessElement
  public void processElement(@Element BaseEvent event, MultiOutputReceiver out) {
    ValidationResult result = validate(event);

    if (result.isValid()) {
      out.get(VALID_TAG).output(event);
    } else {
      DlqEnvelope dlq = new DlqEnvelope();
      // Serialize back to JSON so the DLQ record is replay-friendly.
      String rawPayload;
      try {
        rawPayload = JsonUtils.getMapper().writeValueAsString(event);
      } catch (Exception serEx) {
        rawPayload = event != null ? event.toString() : "";
      }
      dlq.setOriginalPayload(rawPayload);
      dlq.setErrorCategory(result.getCategory());
      dlq.setErrorCode(result.getErrorCode());
      dlq.setErrorMessage(String.join("; ", result.getErrors()));
      dlq.setPipelineStage("ValidateEventFn");
      if (event != null) {
        dlq.setCorrelationId(event.getCorrelationId());
        dlq.setKafkaTopic(event.getKafkaTopic());
        dlq.setKafkaPartition(event.getKafkaPartition());
        dlq.setKafkaOffset(event.getKafkaOffset());
        dlq.setDlqKey(IdempotencyKeyBuilder.buildDlqKey(
            "ValidateEventFn",
            event.getKafkaTopic(),
            event.getKafkaPartition(),
            event.getKafkaOffset(),
            rawPayload));
      }
      dlq.setProcessingTimestamp(Instant.now());

      out.get(DLQ_TAG).output(dlq);
    }
  }

  private ValidationResult validate(BaseEvent event) {
    if (event == null) {
      return ValidationResult.error(ErrorCategory.SCHEMA_VALIDATION_ERROR, "ERR_NULL_EVENT", "Event must not be null");
    }
    if (event.getEventType() == null) {
      return ValidationResult.error(ErrorCategory.SCHEMA_VALIDATION_ERROR, "ERR_MISSING_TYPE", "Event type must not be null");
    }
    if (event.getEventId() == null) {
      return ValidationResult.error(ErrorCategory.SCHEMA_VALIDATION_ERROR, "ERR_MISSING_ID", "Event ID must not be null");
    }
    if (event.getCustomerId() == null || event.getCustomerId().trim().isEmpty()) {
      return ValidationResult.error(ErrorCategory.SCHEMA_VALIDATION_ERROR, "ERR_MISSING_CUST", "Customer ID must not be empty");
    }
    if (event.getEventTime() == null) {
      return ValidationResult.error(ErrorCategory.SCHEMA_VALIDATION_ERROR, "ERR_MISSING_EVENT_TIME", "Event time must not be null");
    }
    Instant now = Instant.now();
    Instant minAllowed = now.minus(Duration.ofDays(maxEventAgeDays));
    Instant maxAllowed = now.plus(Duration.ofHours(1));
    if (event.getEventTime().isBefore(minAllowed) || event.getEventTime().isAfter(maxAllowed)) {
      return ValidationResult.error(
          ErrorCategory.BUSINESS_RULE_VIOLATION,
          "ERR_EVENT_TIME_OUT_OF_RANGE",
          "Event time is outside allowed range [now-7d, now+1h]: " + event.getEventTime());
    }
    if (event.getAmount() != null && event.getAmount().compareTo(BigDecimal.ZERO) < 0) {
      return ValidationResult.error(ErrorCategory.BUSINESS_RULE_VIOLATION, "ERR_NEGATIVE_AMOUNT", "Amount cannot be negative");
    }
    return ValidationResult.ok();
  }
}
