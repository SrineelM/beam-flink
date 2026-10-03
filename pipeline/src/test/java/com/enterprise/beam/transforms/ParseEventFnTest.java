package com.enterprise.beam.transforms;

import com.enterprise.beam.model.BaseEvent;
import com.enterprise.beam.model.DlqEnvelope;
import com.enterprise.beam.model.OrderCreatedEvent;
import com.enterprise.beam.model.RawRecord;
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
import java.time.Instant;

public class ParseEventFnTest implements Serializable {

  private static final long serialVersionUID = 1L;

  @Test
  public void testParseValidOrderCreatedEvent() {
    Pipeline pipeline = Pipeline.create();
    String validJson = "{"
        + "\"event_id\":\"11111111-1111-1111-1111-111111111111\","
        + "\"event_type\":\"ORDER_CREATED\","
        + "\"customer_id\":\"CUST-101\","
        + "\"amount\":99.99,"
        + "\"order_id\":\"ORD-500\","
        + "\"event_time\":\"" + Instant.now().toString() + "\""
        + "}";

    RawRecord record = new RawRecord(validJson, "orders", 1, 100L, System.currentTimeMillis());
    PCollection<RawRecord> input = pipeline.apply(Create.of(record));

    PCollectionTuple result = input.apply(
        ParDo.of(new ParseEventFn())
            .withOutputTags(ParseEventFn.VALID_TAG, TupleTagList.of(ParseEventFn.DLQ_TAG)));

    PCollection<BaseEvent> validEvents = result.get(ParseEventFn.VALID_TAG);

    PAssert.that(validEvents).satisfies(new org.apache.beam.sdk.transforms.SerializableFunction<Iterable<BaseEvent>, Void>() {
      @Override
      public Void apply(Iterable<BaseEvent> events) {
        BaseEvent event = events.iterator().next();
        Assertions.assertNotNull(event);
        Assertions.assertEquals("CUST-101", event.getCustomerId());
        Assertions.assertTrue(event instanceof OrderCreatedEvent);
        Assertions.assertEquals("orders", event.getKafkaTopic());
        Assertions.assertEquals(1, event.getKafkaPartition());
        Assertions.assertEquals(100L, event.getKafkaOffset());
        return null;
      }
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testParseMalformedJsonRoutesToDlq() {
    Pipeline pipeline = Pipeline.create();
    String malformedJson = "{invalid-json-payload";
    RawRecord record = new RawRecord(malformedJson, "orders", 2, 200L, System.currentTimeMillis());

    PCollection<RawRecord> input = pipeline.apply(Create.of(record));

    PCollectionTuple result = input.apply(
        ParDo.of(new ParseEventFn())
            .withOutputTags(ParseEventFn.VALID_TAG, TupleTagList.of(ParseEventFn.DLQ_TAG)));

    PCollection<DlqEnvelope> dlqEvents = result.get(ParseEventFn.DLQ_TAG);

    PAssert.that(dlqEvents).satisfies(new org.apache.beam.sdk.transforms.SerializableFunction<Iterable<DlqEnvelope>, Void>() {
      @Override
      public Void apply(Iterable<DlqEnvelope> dlq) {
        DlqEnvelope envelope = dlq.iterator().next();
        Assertions.assertNotNull(envelope);
        Assertions.assertEquals(malformedJson, envelope.getOriginalPayload());
        Assertions.assertEquals("ParseEventFn", envelope.getPipelineStage());
        Assertions.assertEquals("orders", envelope.getKafkaTopic());
        Assertions.assertEquals(2, envelope.getKafkaPartition());
        Assertions.assertEquals(200L, envelope.getKafkaOffset());
        Assertions.assertNotNull(envelope.getDlqKey());
        return null;
      }
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testEmptyPayloadSkippedSilently() {
    Pipeline pipeline = Pipeline.create();

    RawRecord record = new RawRecord("", "orders", 0, 0L, System.currentTimeMillis());
    PCollection<RawRecord> input = pipeline.apply(Create.of(record));

    PCollectionTuple result = input.apply(
        ParDo.of(new ParseEventFn())
            .withOutputTags(ParseEventFn.VALID_TAG, TupleTagList.of(ParseEventFn.DLQ_TAG)));

    PAssert.that(result.get(ParseEventFn.VALID_TAG)).empty();
    PAssert.that(result.get(ParseEventFn.DLQ_TAG)).empty();

    pipeline.run().waitUntilFinish();
  }
}
