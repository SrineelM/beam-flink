package com.enterprise.beam.util;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Utility for executing operations with exponential backoff and randomized jitter.
 */
public class RetryUtil {

  private RetryUtil() {
  }

  public static <T> T executeWithRetry(
      Callable<T> callable,
      int maxAttempts,
      long baseDelayMs,
      long maxDelayMs,
      double jitterFactor,
      Class<? extends Exception> retryableException) throws Exception {
    return executeWithRetry(
        callable,
        maxAttempts,
        baseDelayMs,
        maxDelayMs,
        jitterFactor,
        retryableException::isInstance);
  }

  /**
   * Executes a callable with exponential backoff, jitter, and custom retry predicate.
   */
  public static <T> T executeWithRetry(
      Callable<T> callable,
      int maxAttempts,
      long baseDelayMs,
      long maxDelayMs,
      double jitterFactor,
      java.util.function.Predicate<Exception> isRetryable) throws Exception {

    int attempt = 0;
    long currentDelay = baseDelayMs;

    while (true) {
      attempt++;
      try {
        return callable.call();
      } catch (Exception e) {
        if (attempt >= maxAttempts || !isRetryable.test(e)) {
          throw e;
        }

        double jitter = (ThreadLocalRandom.current().nextDouble() * 2 - 1) * jitterFactor * currentDelay;
        long backoff = Math.min(maxDelayMs, (long) (currentDelay + jitter));

        try {
          Thread.sleep(Math.max(10L, backoff));
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw e;
        }

        currentDelay = Math.min(maxDelayMs, currentDelay * 2);
      }
    }
  }
}
