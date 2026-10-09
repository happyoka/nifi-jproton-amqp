package com.swim.nifi.amqp.model;

import java.util.Map;

/**
 * Result of a receive operation that includes a FlowFile-ready payload and the acknowledgement
 * contract needed after NiFi decides the session outcome.
 *
 * @param body payload bytes
 * @param attributes FlowFile attributes derived from AMQP metadata
 * @param acknowledgement acknowledgement facade for AMQP settlement
 * @param successSettlement settlement mode for successful NiFi repository commits
 * @param retrySettlement settlement mode for retryable failures
 */
public record ConsumedMessage(
    byte[] body,
    Map<String, String> attributes,
    AmqpAcknowledgement acknowledgement,
    SettlementMode successSettlement,
    SettlementMode retrySettlement) {}
