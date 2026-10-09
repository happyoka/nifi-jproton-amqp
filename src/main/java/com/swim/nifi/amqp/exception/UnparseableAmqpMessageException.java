package com.swim.nifi.amqp.exception;

/** Signals a non-retryable AMQP mapping or payload failure. */
public final class UnparseableAmqpMessageException extends RuntimeException {

  /**
   * Creates a new non-retryable exception with a message.
   *
   * @param message detail message describing the malformed or unsupported content
   */
  public UnparseableAmqpMessageException(final String message) {
    super(message);
  }

  /**
   * Creates a new non-retryable exception with a message and cause.
   *
   * @param message detail message describing the malformed or unsupported content
   * @param cause underlying cause
   */
  public UnparseableAmqpMessageException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
