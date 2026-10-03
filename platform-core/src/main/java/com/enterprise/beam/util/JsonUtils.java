package com.enterprise.beam.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Shared thread-safe Jackson ObjectMapper configured for ISO-8601 timestamps and resilience.
 */
public class JsonUtils {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerModule(new JavaTimeModule());
    MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
  }

  private JsonUtils() {
  }

  public static ObjectMapper getMapper() {
    return MAPPER;
  }
}
