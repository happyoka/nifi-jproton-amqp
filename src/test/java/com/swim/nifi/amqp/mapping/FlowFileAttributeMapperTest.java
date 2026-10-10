package com.swim.nifi.amqp.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.swim.nifi.amqp.model.InboundMessage;
import com.swim.nifi.amqp.model.OutboundMessage;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests metadata mapping between FlowFile attributes and AMQP message models. */
class FlowFileAttributeMapperTest {

  /** Verifies that publish-side attributes become the expected outbound AMQP metadata. */
  @Test
  void shouldMapFlowFileAttributesToOutboundMessage() {
    final FlowFileAttributeMapper mapper = new FlowFileAttributeMapper(new ObjectMapper());
    final Map<String, String> attributes = new LinkedHashMap<>();
    attributes.put("amqp.message-id", "msg-1");
    attributes.put("amqp.correlation-id", "corr-1");
    attributes.put("amqp.subject", "subject-1");
    attributes.put("amqp.to", "queue://orders");
    attributes.put("amqp.application.tenant", "acme");
    attributes.put("traceparent", "00-abc-def-01");

    final OutboundMessage outboundMessage =
        mapper.toOutboundMessage(attributes, "hello".getBytes(StandardCharsets.UTF_8), null);

    assertEquals("msg-1", outboundMessage.messageId());
    assertEquals("corr-1", outboundMessage.correlationId());
    assertEquals("subject-1", outboundMessage.subject());
    assertEquals("queue://orders", outboundMessage.address());
    assertEquals("acme", outboundMessage.applicationProperties().get("tenant"));
  }

  /** Verifies that inbound metadata becomes FlowFile attributes with the expected prefixes. */
  @Test
  void shouldMapInboundMessageToFlowFileAttributes() {
    final FlowFileAttributeMapper mapper = new FlowFileAttributeMapper(new ObjectMapper());
    final InboundMessage inboundMessage =
        new InboundMessage(
            "payload".getBytes(StandardCharsets.UTF_8),
            "msg-2",
            "corr-2",
            "subject-2",
            "topic://events",
            null,
            "application/json",
            null,
            true,
            (byte) 5,
            1000L,
            2000L,
            Map.of("tenant", "acme", "traceparent", "00-xyz-uvw-01", "baggage", "k=v"),
            Map.of("partition", 4));

    final Map<String, String> attributes = mapper.toFlowFileAttributes(inboundMessage);

    assertEquals("msg-2", attributes.get("amqp.message-id"));
    assertEquals("corr-2", attributes.get("amqp.correlation-id"));
    assertEquals("subject-2", attributes.get("amqp.subject"));
    assertEquals("acme", attributes.get("amqp.application.tenant"));
    assertEquals("00-xyz-uvw-01", attributes.get("amqp.application.traceparent"));
    assertEquals("4", attributes.get("amqp.delivery-annotation.partition"));
    assertTrue(attributes.containsKey("amqp.header.priority"));
  }
}
