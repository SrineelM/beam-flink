package com.enterprise.beam.model;

/**
 * Categorization of errors encountered during stream processing.
 */
public enum ErrorCategory {
  /** Unparseable JSON, corrupted bytes, or deserialization failure. */
  PARSE_ERROR,

  /** Failed JSON schema or structural field validation (e.g., missing required UUID). */
  SCHEMA_VALIDATION_ERROR,

  /** Violation of business invariants (e.g., negative payment amount). */
  BUSINESS_RULE_VIOLATION,

  /** Non-fatal data quality warning (e.g., timestamp slightly in future due to clock skew). */
  DATA_QUALITY_WARNING,

  /** Failure writing to downstream sink (e.g., PostgreSQL primary key constraint). */
  SINK_WRITE_FAILURE
}
