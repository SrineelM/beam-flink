package com.enterprise.beam.transforms;

import com.enterprise.beam.model.BaseEvent;
import com.enterprise.beam.util.JsonUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.HashMap;
import java.util.Map;

/**
 * DoFn for enriching event details using side input lookup tables.
 *
 * <p>Produces a deep copy of the incoming event (via JSON round-trip) and sets an enriched
 * metadata map on the copy. This is required because Flink objectReuse=true allows the runner
 * to reuse element objects across invocations, and the enriched stream fans out to three
 * consumers; mutating the input would corrupt all downstream reads after the first one.
 */
public class EnrichOrderFn extends DoFn<BaseEvent, BaseEvent> {

  private static final long serialVersionUID = 1L;

  private final PCollectionView<Map<String, String>> catalogSideInput;

  public EnrichOrderFn(PCollectionView<Map<String, String>> catalogSideInput) {
    this.catalogSideInput = catalogSideInput;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    BaseEvent event = c.element();
    Map<String, String> catalog = c.sideInput(catalogSideInput);

    // Deep-copy the event via JSON round-trip so that setting the enriched metadata
    // does not mutate the original element reference. The @JsonTypeInfo on BaseEvent
    // ensures the correct concrete subtype is deserialized.
    String json = JsonUtils.getMapper().writeValueAsString(event);
    BaseEvent enriched = JsonUtils.getMapper().readValue(json, BaseEvent.class);

    Map<String, String> enrichedMetadata = new HashMap<>(enriched.getMetadata());
    if (enriched.getProductId() != null && catalog.containsKey(enriched.getProductId())) {
      enrichedMetadata.put("category", catalog.get(enriched.getProductId()));
    } else {
      enrichedMetadata.put("category", "General");
    }
    enriched.setMetadata(enrichedMetadata);

    c.output(enriched);
  }
}
