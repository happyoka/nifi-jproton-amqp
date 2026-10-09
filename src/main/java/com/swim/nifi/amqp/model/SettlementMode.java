package com.swim.nifi.amqp.model;

/**
 * AMQP delivery outcomes used by the consume processor when settling messages after NiFi session
 * commit outcomes are known.
 */
public enum SettlementMode {

  /** Marks the delivery as successfully processed and settled. */
  ACCEPTED,

  /** Marks the delivery as definitively failed and settled. */
  REJECTED,

  /** Releases the delivery so the broker may redeliver it later. */
  RELEASED
}
