package com.enterprise.beam.transforms;

import com.enterprise.beam.model.BaseEvent;
import com.enterprise.beam.model.OrderCreatedEvent;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Unit tests for {@link EnrichOrderFn}.
 *
 * <p>T1.6: Verifies that {@code EnrichOrderFn} does NOT mutate the input element. On Flink
 * with {@code objectReuse=true}, mutating the input element would corrupt other downstream
 * DoFns that share the same object reference.
 */
public class EnrichOrderFnTest {

  /**
   * T1.6: Verifies that the original input event is not mutated when the enrichment is applied.
   * The input event's metadata map must not contain the "category" key after enrichment.
   * (The enriched copy's metadata must contain it, but the original must be unchanged.)
   */
  @Test
  public void testEnrichDoesNotMutateInputElement() {
    Pipeline pipeline = Pipeline.create();

    OrderCreatedEvent event = new OrderCreatedEvent();
    event.setEventId(UUID.fromString("00000000-0000-0000-0000-000000000099"));
    event.setCustomerId("CUST-1");
    event.setProductId("PROD-1");
    event.setRegion("US");
    event.setAmount(BigDecimal.valueOf(100));
    event.setEventTime(java.time.Instant.now());

    PCollection<BaseEvent> input = pipeline.apply("Input", Create.<BaseEvent>of(event));

    PCollectionView<Map<String, String>> catalogView = pipeline
        .apply("Catalog", Create.of(KV.of("PROD-1", "Electronics")))
        .apply("CatalogView", View.asMap());

    PCollection<BaseEvent> enriched = input.apply("Enrich",
        ParDo.of(new EnrichOrderFn(catalogView)).withSideInputs(catalogView));

    // 1. Enriched output must have "category" in metadata.
    PAssert.that(enriched).satisfies(events -> {
      int count = 0;
      for (BaseEvent e : events) {
        count++;
        Assertions.assertEquals("Electronics", e.getMetadata().get("category"),
            "Enriched event must have 'category' = 'Electronics' in metadata");
        // Verify the event_id is preserved through the JSON round-trip
        Assertions.assertEquals("00000000-0000-0000-0000-000000000099", e.getEventId().toString());
        Assertions.assertEquals("PROD-1", e.getProductId());
        Assertions.assertEquals("CUST-1", e.getCustomerId());
      }
      Assertions.assertEquals(1, count, "Exactly 1 enriched event must be emitted");
      return null;
    });

    pipeline.run().waitUntilFinish();

    // Verify the original event in the JVM was not mutated by EnrichOrderFn.
    // (This tests the Java side-effect; the Beam-level test above covers the pipeline output.)
    Assertions.assertFalse(event.getMetadata().containsKey("category"),
        "T1.6: EnrichOrderFn must not mutate the original input event's metadata map. "
            + "Actual metadata: " + event.getMetadata());
  }

  /**
   * T1.6: Verifies that events with no matching product in the catalog get category = "General".
   */
  @Test
  public void testEnrichSetsCategoryToGeneralForUnknownProduct() {
    Pipeline pipeline = Pipeline.create();

    OrderCreatedEvent event = new OrderCreatedEvent();
    event.setEventId(UUID.randomUUID());
    event.setCustomerId("CUST-2");
    event.setProductId("PROD-UNKNOWN");
    event.setRegion("EU");
    event.setAmount(BigDecimal.valueOf(50));
    event.setEventTime(java.time.Instant.now());

    PCollection<BaseEvent> input = pipeline.apply("Input2", Create.<BaseEvent>of(event));

    PCollectionView<Map<String, String>> catalogView = pipeline
        .apply("Catalog2", Create.of(KV.of("PROD-1", "Electronics")))
        .apply("CatalogView2", View.asMap());

    PCollection<BaseEvent> enriched = input.apply("Enrich2",
        ParDo.of(new EnrichOrderFn(catalogView)).withSideInputs(catalogView));

    PAssert.that(enriched).satisfies(events -> {
      for (BaseEvent e : events) {
        Assertions.assertEquals("General", e.getMetadata().get("category"),
            "Unknown product must get category = 'General'");
      }
      return null;
    });

    pipeline.run().waitUntilFinish();
  }
}
