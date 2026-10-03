package com.enterprise.beam.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Structured JSON logger wrapper that maintains consistent MDC context key-values.
 */
public class StructuredLogger {

  private final Logger logger;
  private final String component;

  public StructuredLogger(Class<?> clazz, String component) {
    this.logger = LoggerFactory.getLogger(clazz);
    this.component = component;
  }

  public static StructuredLogger getLogger(Class<?> clazz, String component) {
    return new StructuredLogger(clazz, component);
  }

  public void info(String message, String eventId, String correlationId) {
    try (MDC.MDCCloseable c1 = MDC.putCloseable("component", component);
         MDC.MDCCloseable c2 = MDC.putCloseable("event_id", eventId != null ? eventId : "");
         MDC.MDCCloseable c3 = MDC.putCloseable("correlation_id", correlationId != null ? correlationId : "")) {
      logger.info(message);
    }
  }

  public void error(String message, String errorCode, Throwable t) {
    try (MDC.MDCCloseable c1 = MDC.putCloseable("component", component);
         MDC.MDCCloseable c2 = MDC.putCloseable("error_code", errorCode != null ? errorCode : "UNKNOWN")) {
      logger.error(message, t);
    }
  }

  public void warn(String message, String warningCode) {
    try (MDC.MDCCloseable c1 = MDC.putCloseable("component", component);
         MDC.MDCCloseable c2 = MDC.putCloseable("warning_code", warningCode != null ? warningCode : "WARN")) {
      logger.warn(message);
    }
  }
}
