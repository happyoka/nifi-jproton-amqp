package com.swim.nifi.amqp.exception;

/** Signals a transient AMQP or repository fault that should be routed to retry. */
public final class RetryableAmqpException extends RuntimeException {

  /**
   * Creates a new retryable exception with a message.
   *
   * @param message detail message describing the transient failure
   */
  public RetryableAmqpException(final String message) {
    super(message);
  }

  /**
   * Creates a new retryable exception with a message and cause.
   *
   * @param message detail message describing the transient failure
   * @param cause underlying cause
   */
  public RetryableAmqpException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
