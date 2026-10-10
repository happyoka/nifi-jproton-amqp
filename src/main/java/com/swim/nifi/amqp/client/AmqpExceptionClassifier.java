package com.swim.nifi.amqp.client;

import com.swim.nifi.amqp.exception.RetryableAmqpException;
import java.io.IOException;

/** Heuristics for deciding whether an AMQP transport failure should be retried. */
public final class AmqpExceptionClassifier {

  private AmqpExceptionClassifier() {}

  /**
   * Returns whether the supplied exception appears transient and should be routed to retry.
   *
   * @param throwable exception to classify
   * @return {@code true} when the fault looks transient
   */
  public static boolean isRetryable(final Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      if (current instanceof RetryableAmqpException || current instanceof IOException) {
        return true;
      }
      final String typeName = current.getClass().getName().toLowerCase();
      final String message = current.getMessage() == null ? "" : current.getMessage().toLowerCase();
      if (typeName.contains("timeout")
          || typeName.contains("interrupt")
          || message.contains("interrupted by server")
          || message.contains("server interrupted")
          || typeName.contains("connection")
          || ((current instanceof IllegalStateException || typeName.contains("illegalstate"))
              && message.contains("closed"))) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }
}
