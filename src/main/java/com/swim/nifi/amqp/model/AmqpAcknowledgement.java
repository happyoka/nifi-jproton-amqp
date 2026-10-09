package com.swim.nifi.amqp.model;

/**
 * Minimal acknowledgement facade used by the consume processor to settle received AMQP deliveries
 * after session commit callbacks complete.
 */
public interface AmqpAcknowledgement {

  /**
   * Settles the underlying delivery using the requested outcome.
   *
   * @param settlementMode settlement mode to apply
   */
  void settle(SettlementMode settlementMode);
}
