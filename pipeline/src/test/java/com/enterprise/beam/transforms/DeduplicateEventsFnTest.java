package com.enterprise.beam.transforms;

import com.enterprise.beam.model.BaseEvent;
import com.enterprise.beam.model.OrderCreatedEvent;
import com.enterprise.beam.model.ProductAddedToCartEvent;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.UUID;

/**
 * Unit tests for {@link DeduplicateEventsFn}.
 */
public class DeduplicateEventsFnTest implements Serializable {

  private static final long serialVersionUID = 1L;

  @Test
  public void testFirstSeenEventPassesThrough() {
    Pipeline pipeline = Pipeline.create();

    UUID eventId = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001");
    OrderCreatedEvent event = new OrderCreatedEvent();
    event.setEventId(eventId);
    event.setCustomerId("CUST-DEDUP-1");
    event.setAmount(new BigDecimal("75.00"));
    event.setEventTime(java.time.Instant.now());

    PCollection<BaseEvent> deduped = pipeline
        .apply("CreateInput", Create.of((BaseEvent) event))
        .apply("KeyByEventId",
            WithKeys.of((BaseEvent e) -> e.getEventId().toString())
                    .withKeyType(TypeDescriptors.strings()))
        .apply("Deduplicate", ParDo.of(new DeduplicateEventsFn()));

    PAssert.that(deduped).satisfies(events -> {
      int count = 0;
      for (BaseEvent e : events) {
        count++;
        Assertions.assertEquals(eventId, e.getEventId());
      }
      Assertions.assertEquals(1, count, "Expected exactly 1 unique event");
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testDuplicateEventIsSuppressed() {
    Pipeline pipeline = Pipeline.create();

    UUID sharedId = UUID.fromString("bbbbbbbb-0000-0000-0000-000000000002");

    OrderCreatedEvent first = new OrderCreatedEvent();
    first.setEventId(sharedId);
    first.setCustomerId("CUST-DEDUP-2");
    first.setAmount(new BigDecimal("100.00"));
    first.setEventTime(java.time.Instant.now());

    ProductAddedToCartEvent duplicate = new ProductAddedToCartEvent();
    duplicate.setEventId(sharedId);
    duplicate.setCustomerId("CUST-DEDUP-2");
    duplicate.setEventTime(java.time.Instant.now());

    PCollection<KV<String, BaseEvent>> keyed = pipeline
        .apply("CreateDuplicates",
            Create.of((BaseEvent) first, duplicate)
                  .withCoder(SerializableCoder.of(BaseEvent.class)))
        .apply("KeyByEventId",
            WithKeys.of((BaseEvent e) -> e.getEventId().toString())
                    .withKeyType(TypeDescriptors.strings()));

    PCollection<BaseEvent> deduped = keyed.apply("Deduplicate", ParDo.of(new DeduplicateEventsFn()));

    PAssert.that(deduped).satisfies(events -> {
      int count = 0;
      for (BaseEvent ignored : events) {
        count++;
      }
      Assertions.assertEquals(1, count,
          "Duplicate event_id should be suppressed; expected 1 output, got " + count);
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testThreeDistinctEventIdsAllPassThrough() {
    Pipeline pipeline = Pipeline.create();

    UUID id1 = UUID.fromString("cccccccc-0000-0000-0000-000000000001");
    UUID id2 = UUID.fromString("cccccccc-0000-0000-0000-000000000002");
    UUID id3 = UUID.fromString("cccccccc-0000-0000-0000-000000000003");

    OrderCreatedEvent e1 = new OrderCreatedEvent();
    e1.setEventId(id1);
    e1.setCustomerId("CUST-DEDUP-3");
    e1.setAmount(new BigDecimal("10.00"));
    e1.setEventTime(java.time.Instant.now());

    OrderCreatedEvent e2 = new OrderCreatedEvent();
    e2.setEventId(id2);
    e2.setCustomerId("CUST-DEDUP-3");
    e2.setAmount(new BigDecimal("20.00"));
    e2.setEventTime(java.time.Instant.now());

    OrderCreatedEvent e3 = new OrderCreatedEvent();
    e3.setEventId(id3);
    e3.setCustomerId("CUST-DEDUP-3");
    e3.setAmount(new BigDecimal("30.00"));
    e3.setEventTime(java.time.Instant.now());

    PCollection<BaseEvent> deduped = pipeline
        .apply("CreateThreeEvents", Create.of((BaseEvent) e1, e2, e3))
        .apply("KeyByEventId",
            WithKeys.of((BaseEvent e) -> e.getEventId().toString())
                    .withKeyType(TypeDescriptors.strings()))
        .apply("Deduplicate", ParDo.of(new DeduplicateEventsFn()));

    PAssert.that(deduped).satisfies(events -> {
      int count = 0;
      for (BaseEvent ignored : events) {
        count++;
      }
      Assertions.assertEquals(3, count,
          "All 3 distinct event_ids should pass through; got " + count);
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testDeduplicationTtlExpiryWithTestStream() {
    Pipeline pipeline = Pipeline.create();

    Duration ttl = Duration.standardSeconds(5);
    UUID eventId = UUID.fromString("dddddddd-0000-0000-0000-000000000001");
    String key = eventId.toString();

    OrderCreatedEvent e1 = new OrderCreatedEvent();
    e1.setEventId(eventId);
    e1.setCustomerId("CUST-TTL");
    e1.setAmount(new BigDecimal("10.00"));
    e1.setEventTime(java.time.Instant.now());

    OrderCreatedEvent eDuplicate = new OrderCreatedEvent();
    eDuplicate.setEventId(eventId);
    eDuplicate.setCustomerId("CUST-TTL");
    eDuplicate.setAmount(new BigDecimal("20.00"));
    eDuplicate.setEventTime(java.time.Instant.now());

    OrderCreatedEvent eAfterTtl = new OrderCreatedEvent();
    eAfterTtl.setEventId(eventId);
    eAfterTtl.setCustomerId("CUST-TTL");
    eAfterTtl.setAmount(new BigDecimal("30.00"));
    eAfterTtl.setEventTime(java.time.Instant.now());

    TestStream<KV<String, BaseEvent>> stream = TestStream
        .create(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(BaseEvent.class)))
        .advanceWatermarkTo(new Instant(0))
        // 1. First event passes
        .addElements(KV.of(key, (BaseEvent) e1))
        // Advance processing time by 1s (within TTL)
        .advanceProcessingTime(Duration.standardSeconds(1))
        // 2. Duplicate within TTL is dropped
        .addElements(KV.of(key, (BaseEvent) eDuplicate))
        // Advance processing time by 10s (past the 5s TTL)
        .advanceProcessingTime(Duration.standardSeconds(10))
        // 3. After TTL has expired, same ID passes again
        .addElements(KV.of(key, (BaseEvent) eAfterTtl))
        .advanceWatermarkToInfinity();

    PCollection<BaseEvent> deduped = pipeline
        .apply("TestStream", stream)
        .apply("DeduplicateWithTtl", ParDo.of(new DeduplicateEventsFn(ttl)));

    PAssert.that(deduped).satisfies(events -> {
      int count = 0;
      BigDecimal sum = BigDecimal.ZERO;
      for (BaseEvent e : events) {
        count++;
        sum = sum.add(e.getAmount());
      }
      // Should have e1 (10.00) and eAfterTtl (30.00), eDuplicate (20.00) dropped
      Assertions.assertEquals(2, count, "Expected 2 events (first and after-TTL)");
      Assertions.assertEquals(new BigDecimal("40.00"), sum);
      return null;
    });

    pipeline.run().waitUntilFinish();
  }
}
