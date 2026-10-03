package com.enterprise.beam.transforms;

import com.enterprise.beam.model.BaseEvent;
import com.enterprise.beam.model.DlqEnvelope;
import com.enterprise.beam.model.ErrorCategory;
import com.enterprise.beam.model.OrderCreatedEvent;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

public class ValidateEventFnTest implements Serializable {

  private static final long serialVersionUID = 1L;

  @Test
  public void testValidEventPassesValidation() {
    Pipeline pipeline = Pipeline.create();
    OrderCreatedEvent validEvent = new OrderCreatedEvent();
    validEvent.setEventId(UUID.randomUUID());
    validEvent.setCustomerId("CUST-200");
    validEvent.setAmount(new BigDecimal("150.00"));
    validEvent.setEventTime(Instant.now());

    PCollection<BaseEvent> input = pipeline.apply(Create.of((BaseEvent) validEvent));

    PCollectionTuple result = input.apply(
        ParDo.of(new ValidateEventFn())
            .withOutputTags(ValidateEventFn.VALID_TAG, TupleTagList.of(ValidateEventFn.DLQ_TAG)));

    PAssert.that(result.get(ValidateEventFn.VALID_TAG)).containsInAnyOrder(validEvent);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testMissingCustomerIdRoutesToDlq() {
    Pipeline pipeline = Pipeline.create();
    OrderCreatedEvent invalidEvent = new OrderCreatedEvent();
    invalidEvent.setEventId(UUID.randomUUID());
    invalidEvent.setCustomerId(""); // Empty customer ID
    invalidEvent.setAmount(new BigDecimal("50.00"));
    invalidEvent.setEventTime(Instant.now());

    PCollection<BaseEvent> input = pipeline.apply(Create.of((BaseEvent) invalidEvent));

    PCollectionTuple result = input.apply(
        ParDo.of(new ValidateEventFn())
            .withOutputTags(ValidateEventFn.VALID_TAG, TupleTagList.of(ValidateEventFn.DLQ_TAG)));

    PCollection<DlqEnvelope> dlq = result.get(ValidateEventFn.DLQ_TAG);

    PAssert.that(dlq).satisfies(new org.apache.beam.sdk.transforms.SerializableFunction<Iterable<DlqEnvelope>, Void>() {
      @Override
      public Void apply(Iterable<DlqEnvelope> records) {
        DlqEnvelope envelope = records.iterator().next();
        Assertions.assertNotNull(envelope);
        Assertions.assertEquals("ValidateEventFn", envelope.getPipelineStage());
        Assertions.assertEquals("ERR_MISSING_CUST", envelope.getErrorCode());
        return null;
      }
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testNegativeAmountRoutesToDlq() {
    Pipeline pipeline = Pipeline.create();
    OrderCreatedEvent invalidEvent = new OrderCreatedEvent();
    invalidEvent.setEventId(UUID.randomUUID());
    invalidEvent.setCustomerId("CUST-NEG-1");
    invalidEvent.setAmount(new BigDecimal("-0.01")); // negative amount
    invalidEvent.setEventTime(Instant.now());

    PCollection<BaseEvent> input = pipeline.apply(Create.of((BaseEvent) invalidEvent));

    PCollectionTuple result = input.apply(
        ParDo.of(new ValidateEventFn())
            .withOutputTags(ValidateEventFn.VALID_TAG, TupleTagList.of(ValidateEventFn.DLQ_TAG)));

    PCollection<DlqEnvelope> dlq = result.get(ValidateEventFn.DLQ_TAG);

    PAssert.that(dlq).satisfies(new org.apache.beam.sdk.transforms.SerializableFunction<Iterable<DlqEnvelope>, Void>() {
      @Override
      public Void apply(Iterable<DlqEnvelope> records) {
        DlqEnvelope envelope = records.iterator().next();
        Assertions.assertNotNull(envelope);
        Assertions.assertEquals("ValidateEventFn", envelope.getPipelineStage());
        Assertions.assertEquals(
            ErrorCategory.BUSINESS_RULE_VIOLATION,
            envelope.getErrorCategory(),
            "Negative amount should produce BUSINESS_RULE_VIOLATION, not SCHEMA_VALIDATION_ERROR");
        Assertions.assertEquals("ERR_NEGATIVE_AMOUNT", envelope.getErrorCode());
        return null;
      }
    });

    PAssert.that(result.get(ValidateEventFn.VALID_TAG)).empty();

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testMissingEventTimeRoutesToDlq() {
    Pipeline pipeline = Pipeline.create();
    OrderCreatedEvent invalidEvent = new OrderCreatedEvent();
    invalidEvent.setEventId(UUID.randomUUID());
    invalidEvent.setCustomerId("CUST-NO-TIME");
    invalidEvent.setAmount(new BigDecimal("10.00"));
    invalidEvent.setEventTime(null); // Missing event_time

    PCollection<BaseEvent> input = pipeline.apply(Create.of((BaseEvent) invalidEvent));

    PCollectionTuple result = input.apply(
        ParDo.of(new ValidateEventFn())
            .withOutputTags(ValidateEventFn.VALID_TAG, TupleTagList.of(ValidateEventFn.DLQ_TAG)));

    PAssert.that(result.get(ValidateEventFn.DLQ_TAG)).satisfies(records -> {
      DlqEnvelope envelope = records.iterator().next();
      Assertions.assertEquals("ERR_MISSING_EVENT_TIME", envelope.getErrorCode());
      Assertions.assertEquals(ErrorCategory.SCHEMA_VALIDATION_ERROR, envelope.getErrorCategory());
      return null;
    });

    PAssert.that(result.get(ValidateEventFn.VALID_TAG)).empty();

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testEventTimeOutOfRangeRoutesToDlq() {
    Pipeline pipeline = Pipeline.create();
    OrderCreatedEvent staleEvent = new OrderCreatedEvent();
    staleEvent.setEventId(UUID.randomUUID());
    staleEvent.setCustomerId("CUST-STALE");
    staleEvent.setAmount(new BigDecimal("10.00"));
    // Older than 7 days
    staleEvent.setEventTime(Instant.now().minus(Duration.ofDays(8)));

    PCollection<BaseEvent> input = pipeline.apply(Create.of((BaseEvent) staleEvent));

    PCollectionTuple result = input.apply(
        ParDo.of(new ValidateEventFn())
            .withOutputTags(ValidateEventFn.VALID_TAG, TupleTagList.of(ValidateEventFn.DLQ_TAG)));

    PAssert.that(result.get(ValidateEventFn.DLQ_TAG)).satisfies(records -> {
      DlqEnvelope envelope = records.iterator().next();
      Assertions.assertEquals("ERR_EVENT_TIME_OUT_OF_RANGE", envelope.getErrorCode());
      Assertions.assertEquals(ErrorCategory.BUSINESS_RULE_VIOLATION, envelope.getErrorCategory());
      return null;
    });

    PAssert.that(result.get(ValidateEventFn.VALID_TAG)).empty();

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testMissingEventTypeRoutesToDlq() {
    Pipeline pipeline = Pipeline.create();
    OrderCreatedEvent noTypeEvent = new OrderCreatedEvent();
    noTypeEvent.setEventId(UUID.randomUUID());
    noTypeEvent.setCustomerId("CUST-NO-TYPE");
    noTypeEvent.setAmount(new BigDecimal("10.00"));
    noTypeEvent.setEventTime(Instant.now());
    noTypeEvent.setEventType(null); // Missing event_type

    PCollection<BaseEvent> input = pipeline.apply(Create.of((BaseEvent) noTypeEvent));

    PCollectionTuple result = input.apply(
        ParDo.of(new ValidateEventFn())
            .withOutputTags(ValidateEventFn.VALID_TAG, TupleTagList.of(ValidateEventFn.DLQ_TAG)));

    PAssert.that(result.get(ValidateEventFn.DLQ_TAG)).satisfies(records -> {
      DlqEnvelope envelope = records.iterator().next();
      Assertions.assertEquals("ERR_MISSING_TYPE", envelope.getErrorCode());
      Assertions.assertEquals(ErrorCategory.SCHEMA_VALIDATION_ERROR, envelope.getErrorCategory());
      return null;
    });

    PAssert.that(result.get(ValidateEventFn.VALID_TAG)).empty();

    pipeline.run().waitUntilFinish();
  }
}
