package com.swim.nifi.amqp.mapping;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.swim.nifi.amqp.exception.UnparseableAmqpMessageException;
import com.swim.nifi.amqp.model.InboundMessage;
import com.swim.nifi.amqp.model.OutboundMessage;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

/** Maps NiFi FlowFile attributes to AMQP message metadata and back again. */
public final class FlowFileAttributeMapper {

  public static final String AMQP_PREFIX = "amqp.";
  public static final String HEADER_PREFIX = "amqp.header.";
  public static final String APPLICATION_PREFIX = "amqp.application.";
  public static final String DELIVERY_ANNOTATION_PREFIX = "amqp.delivery-annotation.";

  private static final List<String> TELEMETRY_KEYS =
      List.of("traceparent", "tracestate", "baggage");

  private final ObjectMapper objectMapper;

  /**
   * Creates a new mapper.
   *
   * @param objectMapper object mapper used to serialize structured AMQP bodies when needed
   */
  public FlowFileAttributeMapper(final ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  private static void put(
      final Map<String, String> attributes, final String key, final Object value) {
    if (value != null) {
      attributes.put(key, Objects.toString(value));
    }
  }

  private static String resolve(final String preferred, final String fallback) {
    if (preferred != null && !preferred.isBlank()) {
      return preferred;
    }
    return fallback;
  }

  private static Boolean toBoolean(final String value) {
    return value == null ? null : Boolean.valueOf(value);
  }

  private static Byte toByte(final String value) {
    return value == null ? null : Byte.valueOf(value);
  }

  private static Long toLong(final String value) {
    return value == null ? null : Long.valueOf(value);
  }

  private static void copyTelemetryToApplicationProperties(
      final Map<String, String> attributes, final Map<String, Object> applicationProperties) {
    TELEMETRY_KEYS.forEach(
        key -> {
          final String value = attributes.get(key);
          if (value != null) {
            applicationProperties.putIfAbsent(key, value);
          }
        });
  }

  private static void copyTelemetryToCanonicalAttributes(final Map<String, String> attributes) {
    TELEMETRY_KEYS.forEach(
        key -> {
          final String value = attributes.get(APPLICATION_PREFIX + key);
          if (value != null) {
            attributes.put(key, value);
          }
        });
  }

  /**
   * Creates an outbound AMQP message model from FlowFile attributes and content bytes.
   *
   * @param attributes FlowFile attributes
   * @param body FlowFile payload bytes
   * @param addressOverride optional processor-level address override
   * @return outbound message model
   */
  public OutboundMessage toOutboundMessage(
      final Map<String, String> attributes, final byte[] body, final String addressOverride) {
    final Map<String, Object> applicationProperties = new LinkedHashMap<>();
    final Map<String, Object> deliveryAnnotations = new LinkedHashMap<>();

    for (Map.Entry<String, String> entry : attributes.entrySet()) {
      final String key = entry.getKey();
      final String value = entry.getValue();
      if (key.startsWith(APPLICATION_PREFIX)) {
        applicationProperties.put(key.substring(APPLICATION_PREFIX.length()), value);
      } else if (key.startsWith(DELIVERY_ANNOTATION_PREFIX)) {
        deliveryAnnotations.put(key.substring(DELIVERY_ANNOTATION_PREFIX.length()), value);
      }
    }
    copyTelemetryToApplicationProperties(attributes, applicationProperties);

    return new OutboundMessage(
        body,
        resolve(addressOverride, attributes.get("amqp.to")),
        attributes.get("amqp.message-id"),
        toLong(attributes.get("amqp.creationTime")),
        attributes.get("amqp.correlation-id"),
        attributes.get("amqp.subject"),
        attributes.get("amqp.reply-to"),
        attributes.get("amqp.content-type"),
        attributes.get("amqp.content-encoding"),
        applicationProperties,
        deliveryAnnotations,
        toBoolean(attributes.get("amqp.header.durable")),
        toByte(attributes.get("amqp.header.priority")),
        toLong(attributes.get("amqp.header.ttl")));
  }

  /**
   * Populates a ProtonJ2 message from the internal outbound model.
   *
   * @param outboundMessage outbound message model
   * @return ProtonJ2 message instance
   */
  public Message<byte[]> toMessage(final OutboundMessage outboundMessage) {
    final Message<byte[]> message = Message.create(outboundMessage.body());
    try {
      if (outboundMessage.address() != null) {
        message.to(outboundMessage.address());
      }
      if (outboundMessage.messageId() != null) {
        message.messageId(outboundMessage.messageId());
      }
      if (outboundMessage.creationTime() != null) {
        message.creationTime(outboundMessage.creationTime());
      }
      if (outboundMessage.correlationId() != null) {
        message.correlationId(outboundMessage.correlationId());
      }
      if (outboundMessage.subject() != null) {
        message.subject(outboundMessage.subject());
      }
      if (outboundMessage.replyTo() != null) {
        message.replyTo(outboundMessage.replyTo());
      }
      if (outboundMessage.contentType() != null) {
        message.contentType(outboundMessage.contentType());
      }
      if (outboundMessage.contentEncoding() != null) {
        message.contentEncoding(outboundMessage.contentEncoding());
      }
      if (outboundMessage.durable() != null) {
        message.durable(outboundMessage.durable());
      }
      if (outboundMessage.priority() != null) {
        message.priority(outboundMessage.priority());
      }
      if (outboundMessage.timeToLive() != null) {
        message.timeToLive(outboundMessage.timeToLive());
      }
      for (Map.Entry<String, Object> entry : outboundMessage.applicationProperties().entrySet()) {
        message.property(entry.getKey(), entry.getValue());
      }
    } catch (final ClientException exception) {
      throw new UnparseableAmqpMessageException(
          "Failed to map FlowFile attributes to an AMQP message", exception);
    }
    return message;
  }

  /**
   * Converts an inbound AMQP message into a FlowFile-oriented metadata map.
   *
   * @param inboundMessage received message model
   * @return FlowFile attributes
   */
  public Map<String, String> toFlowFileAttributes(final InboundMessage inboundMessage) {
    final Map<String, String> attributes = new LinkedHashMap<>();
    put(attributes, "amqp.message-id", inboundMessage.messageId());
    put(attributes, "amqp.creationTime", inboundMessage.creationTime());
    put(attributes, "amqp.correlation-id", inboundMessage.correlationId());
    put(attributes, "amqp.subject", inboundMessage.subject());
    put(attributes, "amqp.to", inboundMessage.to());
    put(attributes, "amqp.reply-to", inboundMessage.replyTo());
    put(attributes, "amqp.content-type", inboundMessage.contentType());
    put(attributes, "amqp.content-encoding", inboundMessage.contentEncoding());
    put(attributes, "amqp.header.durable", inboundMessage.durable());
    put(attributes, "amqp.header.priority", inboundMessage.priority());
    put(attributes, "amqp.header.ttl", inboundMessage.timeToLive());
    inboundMessage
        .applicationProperties()
        .forEach((key, value) -> put(attributes, APPLICATION_PREFIX + key, value));
    inboundMessage
        .deliveryAnnotations()
        .forEach((key, value) -> put(attributes, DELIVERY_ANNOTATION_PREFIX + key, value));
    copyTelemetryToCanonicalAttributes(attributes);
    return attributes;
  }

  /**
   * Converts an arbitrary inbound AMQP body object to bytes suitable for a FlowFile payload.
   *
   * @param body inbound AMQP body object
   * @return serialized payload bytes
   */
  public byte[] toPayloadBytes(final Object body) {
    if (body == null) {
      return new byte[0];
    }
    if (body instanceof byte[] bytes) {
      return bytes;
    }
    if (body instanceof String stringValue) {
      return stringValue.getBytes(StandardCharsets.UTF_8);
    }
    try {
      return objectMapper.writeValueAsBytes(body);
    } catch (final JsonProcessingException exception) {
      throw new UnparseableAmqpMessageException(
          "Unsupported inbound AMQP body type: " + body.getClass().getName(), exception);
    }
  }
}
