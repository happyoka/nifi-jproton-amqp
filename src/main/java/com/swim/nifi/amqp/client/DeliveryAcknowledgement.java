package com.swim.nifi.amqp.client;

import com.swim.nifi.amqp.exception.RetryableAmqpException;
import com.swim.nifi.amqp.model.AmqpAcknowledgement;
import com.swim.nifi.amqp.model.SettlementMode;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.qpid.protonj2.client.Delivery;

/** AMQP acknowledgement implementation backed by a ProtonJ2 delivery. */
final class DeliveryAcknowledgement implements AmqpAcknowledgement {

  private final Delivery delivery;
  private final AtomicBoolean settled = new AtomicBoolean();

  /**
   * Creates a new acknowledgement wrapper.
   *
   * @param delivery Proton delivery to settle later
   */
  DeliveryAcknowledgement(final Delivery delivery) {
    this.delivery = delivery;
  }

  /**
   * Applies the requested settlement mode to the wrapped delivery.
   *
   * @param settlementMode settlement mode selected from broker profile policy
   */
  @Override
  public void settle(final SettlementMode settlementMode) {
    if (!settled.compareAndSet(false, true)) {
      return;
    }
    Exception failure = null;
    try {
      switch (settlementMode) {
        case ACCEPTED -> delivery.accept();
        case REJECTED ->
            delivery.reject("amqp:processing-failed", "NiFi marked the message as failed");
        case RELEASED -> delivery.release();
      }
    } catch (final Exception exception) {
      failure = exception;
    }
    if (failure != null) {
      throw new RetryableAmqpException("Failed to settle consumed AMQP message", failure);
    }
  }
}
