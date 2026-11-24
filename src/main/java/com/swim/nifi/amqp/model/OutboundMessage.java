package com.swim.nifi.amqp.model;

import java.util.Map;

/**
 * Immutable outbound message model used by handlers before a Proton message instance is created.
 *
 * @param body message body bytes
 * @param address AMQP destination address
 * @param messageId AMQP message-id value
 * @param userId AMQP user-id value
 * @param creationTime creation timestamp header value in milliseconds
 * @param correlationId AMQP correlation-id value
 * @param subject AMQP subject value
 * @param replyTo AMQP reply-to address
 * @param contentType MIME content type for the body
 * @param contentEncoding content encoding for the body
 * @param groupId AMQP group-id value identifying the message group
 * @param groupSequence AMQP group-sequence value indicating sequence within the message group
 * @param replyToGroupId AMQP reply-to-group-id value for group-scoped replies
 * @param applicationProperties AMQP application properties
 * @param deliveryAnnotations AMQP delivery annotations
 * @param durable durable header value
 * @param priority priority header value
 * @param timeToLive time-to-live header value in milliseconds
 * @param payloadKind logical payload kind, such as TEXT or BINARY, carried as metadata
 */
public record OutboundMessage(
    byte[] body,
    String address,
    String userId,
    String messageId,
    Long creationTime,
    String correlationId,
    String subject,
    String replyTo,
    String contentType,
    String contentEncoding,
    String groupId,
    Integer groupSequence,
    String replyToGroupId,
    Map<String, Object> applicationProperties,
    Map<String, Object> deliveryAnnotations,
    Boolean durable,
    Byte priority,
    Long timeToLive,
    String payloadKind) {}
