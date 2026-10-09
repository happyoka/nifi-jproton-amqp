package com.swim.nifi.amqp.handler;

import com.swim.nifi.amqp.client.AmqpClient;
import com.swim.nifi.amqp.mapping.FlowFileAttributeMapper;
import com.swim.nifi.amqp.model.OutboundMessage;
import com.swim.nifi.amqp.support.ExecutionBoundController;
import com.swim.nifi.amqp.support.PayloadGenerator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

/** Encapsulates publish-side payload materialization, metadata mapping, and broker interaction. */
public final class PublishMessageHandler {

  private final ExecutionBoundController executionBoundController;
  private final AmqpClient amqpClient;
  private final FlowFileAttributeMapper flowFileAttributeMapper;
  private final PayloadGenerator payloadGenerator;

  /**
   * Creates a publish handler.
   *
   * @param executionBoundController concurrency guard for broker interactions
   * @param amqpClient transport facade used to publish messages
   * @param flowFileAttributeMapper metadata mapper
   * @param payloadGenerator payload materializer
   */
  public PublishMessageHandler(
      final ExecutionBoundController executionBoundController,
      final AmqpClient amqpClient,
      final FlowFileAttributeMapper flowFileAttributeMapper,
      final PayloadGenerator payloadGenerator) {
    this.executionBoundController = executionBoundController;
    this.amqpClient = amqpClient;
    this.flowFileAttributeMapper = flowFileAttributeMapper;
    this.payloadGenerator = payloadGenerator;
  }

  /**
   * Publishes a FlowFile to the selected broker profile.
   *
   * @param session NiFi process session used to read content and attributes
   * @param flowFile FlowFile being published
   * @param profileName broker profile name
   * @param addressOverride optional processor-level address override
   */
  public void publish(
      final ProcessSession session,
      final FlowFile flowFile,
      final String profileName,
      final String addressOverride) {
    final OutboundMessage outboundMessage =
        flowFileAttributeMapper.toOutboundMessage(
            flowFile.getAttributes(), payloadGenerator.read(session, flowFile), addressOverride);

    executionBoundController.execute(
        () -> {
          amqpClient.publish(profileName, outboundMessage);
          return null;
        });
  }
}
