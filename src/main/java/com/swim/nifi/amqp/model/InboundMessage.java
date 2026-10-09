package com.swim.nifi.amqp.model;

import java.util.Map;

/**
 * Immutable inbound message model extracted from a Proton delivery.
 *
 * @param body payload bytes
 * @param messageId AMQP message-id value
 * @param correlationId AMQP correlation-id value
 * @param subject AMQP subject value
 * @param to AMQP to address
 * @param replyTo AMQP reply-to address
 * @param contentType MIME content type for the body
 * @param contentEncoding content encoding for the body
 * @param durable durable header value
 * @param priority priority header value
 * @param timeToLive time-to-live header value in milliseconds
 * @param creationTime creation timestamp header value in milliseconds
 * @param applicationProperties AMQP application properties
 * @param deliveryAnnotations AMQP delivery annotations
 */
public record InboundMessage(
    byte[] body,
    String messageId,
    String correlationId,
    String subject,
    String to,
    String replyTo,
    String contentType,
    String contentEncoding,
    Boolean durable,
    Byte priority,
    Long timeToLive,
    Long creationTime,
    Map<String, Object> applicationProperties,
    Map<String, Object> deliveryAnnotations) {}
