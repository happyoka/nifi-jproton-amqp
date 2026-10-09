package com.swim.nifi.amqp.client;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.junit.jupiter.api.Test;

class AmqpExceptionClassifierTest {

  @Test
  void shouldTreatClosedSessionStateAsRetryable() {
    final ClientIllegalStateException exception =
        new ClientIllegalStateException("Cannot create new Receiver from closed Session");

    assertTrue(AmqpExceptionClassifier.isRetryable(exception));
  }

  @Test
  void shouldTreatInterruptedByServerMessageAsRetryable() {
    final IllegalStateException exception =
        new IllegalStateException("Connection interrupted by server during receive");

    assertTrue(AmqpExceptionClassifier.isRetryable(exception));
  }

  @Test
  void shouldNotTreatGenericIllegalStateAsRetryable() {
    final IllegalStateException exception = new IllegalStateException("Body decoding failed");

    assertFalse(AmqpExceptionClassifier.isRetryable(exception));
  }
}
