package com.enterprise.beam.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Validation result wrapper returned by event validation logic.
 */
public class ValidationResult implements Serializable {

  private static final long serialVersionUID = 1L;

  private final boolean valid;
  private final ErrorCategory category;
  private final String errorCode;
  private final List<String> errors;
  private final List<String> warnings;

  public ValidationResult(boolean valid, ErrorCategory category, String errorCode, List<String> errors, List<String> warnings) {
    this.valid = valid;
    this.category = category;
    this.errorCode = errorCode;
    this.errors = errors != null ? errors : new ArrayList<>();
    this.warnings = warnings != null ? warnings : new ArrayList<>();
  }

  public static ValidationResult ok() {
    return new ValidationResult(true, null, null, null, null);
  }

  public static ValidationResult warning(String warningMsg) {
    List<String> w = new ArrayList<>();
    w.add(warningMsg);
    return new ValidationResult(true, ErrorCategory.DATA_QUALITY_WARNING, "WARNING", null, w);
  }

  public static ValidationResult error(ErrorCategory category, String errorCode, String errorMsg) {
    List<String> e = new ArrayList<>();
    e.add(errorMsg);
    return new ValidationResult(false, category, errorCode, e, null);
  }

  public boolean isValid() { return valid; }
  public ErrorCategory getCategory() { return category; }
  public String getErrorCode() { return errorCode; }
  public List<String> getErrors() { return errors; }
  public List<String> getWarnings() { return warnings; }
}
