package com.swim.nifi.amqp.model;

import java.util.Map;

/**
 * Immutable outbound message model used by handlers before a Proton message instance is created.
 *
 * @param body message body bytes
 * @param address AMQP destination address
 * @param messageId AMQP message-id value
 * @param creationTime creation timestamp header value in milliseconds
 * @param correlationId AMQP correlation-id value
 * @param subject AMQP subject value
 * @param replyTo AMQP reply-to address
 * @param contentType MIME content type for the body
 * @param contentEncoding content encoding for the body
 * @param applicationProperties AMQP application properties
 * @param deliveryAnnotations AMQP delivery annotations
 * @param durable durable header value
 * @param priority priority header value
 * @param timeToLive time-to-live header value in milliseconds
 */
public record OutboundMessage(
    byte[] body,
    String address,
    String messageId,
    Long creationTime,
    String correlationId,
    String subject,
    String replyTo,
    String contentType,
    String contentEncoding,
    Map<String, Object> applicationProperties,
    Map<String, Object> deliveryAnnotations,
    Boolean durable,
    Byte priority,
    Long timeToLive) {}
