package com.swim.nifi.amqp.mapping;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.swim.nifi.amqp.exception.UnparseableAmqpMessageException;
import com.swim.nifi.amqp.model.InboundMessage;
import com.swim.nifi.amqp.model.OutboundMessage;
import com.swim.nifi.amqp.model.PayloadKindType;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

/** Maps NiFi FlowFile attributes to AMQP message metadata and back again. */
public final class FlowFileAttributeMapper {

  /**
   * The property name prefix used to identify custom application-level message headers or
   * properties that should be extracted from or injected into AMQP messages.
   */
  public static final String APPLICATION_PREFIX = "amqp.application.";

  /**
   * The property name prefix used to identify delivery annotation that should be extracted from or
   * injected into AMQP messages.
   */
  public static final String DELIVERY_ANNOTATION_PREFIX = "amqp.delivery-annotation.";

  /**
   * The property name prefix used to identify body payload kind that should be extracted from or
   * injected into AMQP messages.
   */
  public static final String PAYLOAD_KIND_ATTRIBUTE = "amqp.payload-kind";

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

  /**
   * Resolves the payload kind based on an explicitly provided kind or by inspecting the runtime
   * type of the given body object.
   *
   * <p>Resolution precedence and logic:
   *
   * <ol>
   *   <li>If {@code explicitPayloadKind} is non-null and not blank, it is returned immediately.
   *   <li>If {@code body} is a {@link Character} or {@link CharSequence}, it resolves to {@link
   *       PayloadKindType#TEXT}.
   *   <li>If {@code body} is a {@link Map}, {@link Iterable}, {@link Number}, or {@link Boolean},
   *       it resolves to {@link PayloadKindType#STRUCTURED}.
   *   <li>Otherwise, it defaults to {@link PayloadKindType#BINARY}.
   * </ol>
   *
   * @param body the payload object used for type inference if no explicit kind is given
   * @param explicitPayloadKind an optional explicit payload kind string that takes precedence if
   *     present
   * @return the resolved payload kind name as a string
   */
  public static String resolvePayloadKind(final Object body, final String explicitPayloadKind) {
    if (explicitPayloadKind != null && !explicitPayloadKind.isBlank()) {
      return explicitPayloadKind;
    }
    if (body instanceof Character || body instanceof CharSequence) {
      return PayloadKindType.TEXT.name();
    }
    if (body instanceof Map
        || body instanceof Iterable
        || body instanceof Number
        || body instanceof Boolean) {
      return PayloadKindType.STRUCTURED.name();
    }
    return PayloadKindType.BINARY.name();
  }

  private static String decodeTextPayload(final byte[] body) {
    return new String(body, StandardCharsets.UTF_8);
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
        toLong(attributes.get("amqp.header.ttl")),
        resolvePayloadKind(body, attributes.get(PAYLOAD_KIND_ATTRIBUTE)));
  }

  /**
   * Populates a ProtonJ2 message from the internal outbound model.
   *
   * @param outboundMessage outbound message model
   * @return ProtonJ2 message instance
   */
  public Message<?> toMessage(final OutboundMessage outboundMessage) {
    final boolean emitTextBody =
        PayloadKindType.TEXT.name().equalsIgnoreCase(outboundMessage.payloadKind());
    final Message<?> message =
        emitTextBody
            ? Message.create(decodeTextPayload(outboundMessage.body()))
            : Message.create(outboundMessage.body());
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
    put(attributes, PAYLOAD_KIND_ATTRIBUTE, inboundMessage.payloadKind());
    inboundMessage
        .applicationProperties()
        .forEach((key, value) -> put(attributes, APPLICATION_PREFIX + key, value));
    inboundMessage
        .deliveryAnnotations()
        .forEach((key, value) -> put(attributes, DELIVERY_ANNOTATION_PREFIX + key, value));
    return attributes;
  }

  /**
   * Converts an arbitrary inbound AMQP body object to bytes suitable for a FlowFile payload.
   *
   * <p>The canonical wire and FlowFile payload remains a byte[]; String is treated as a
   * special-case optimization that preserves text semantics while encoding to UTF-8 bytes before
   * the payload is persisted or sent. All non-String values continue to follow the existing
   * binary/serialized byte[] contract.
   *
   * @param body inbound AMQP body object
   * @return serialized payload bytes
   */
  public byte[] toPayloadBytes(final Object body) {
    switch (body) {
      case null -> {
        return new byte[0];
      }
      case byte[] bytes -> {
        return bytes;
      }
      case String stringValue -> {
        return stringValue.getBytes(StandardCharsets.UTF_8);
      }
      default -> {}
    }
    try {
      return objectMapper.writeValueAsBytes(body);
    } catch (final JsonProcessingException exception) {
      throw new UnparseableAmqpMessageException(
          "Unsupported inbound AMQP body type: " + body.getClass().getName(), exception);
    }
  }
}
