package com.swim.nifi.amqp.handler;

import com.swim.nifi.amqp.client.AmqpClient;
import com.swim.nifi.amqp.model.ConsumedMessage;
import com.swim.nifi.amqp.support.ExecutionBoundController;
import java.util.Optional;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

/** Encapsulates consume-side broker polling, metadata translation, and FlowFile materialization. */
public final class ConsumeMessageHandler {
  private final ExecutionBoundController executionBoundController;
  private final AmqpClient amqpClient;

  /**
   * Creates a consume handler.
   *
   * @param executionBoundController concurrency guard for broker interactions
   * @param amqpClient transport facade used to receive messages
   */
  public ConsumeMessageHandler(
      final ExecutionBoundController executionBoundController, final AmqpClient amqpClient) {
    this.executionBoundController = executionBoundController;
    this.amqpClient = amqpClient;
  }

  /**
   * Establishes a long-lived subscription for efficient message consumption across multiple
   * triggers.
   *
   * @param profileName broker profile name
   * @param addressOverride optional processor-level address override
   */
  public void subscribe(final String profileName, final String addressOverride) {
    executionBoundController.execute(
        () -> {
          amqpClient.subscribe(profileName, addressOverride);
          return null;
        });
  }

  /**
   * Polls an active long-lived subscription for the next available message.
   *
   * @param profileName broker profile name
   * @return optional consumed message
   */
  public Optional<ConsumedMessage> poll(final String profileName) {
    return executionBoundController.execute(() -> amqpClient.poll(profileName));
  }

  /**
   * Closes a long-lived subscription and releases its resources.
   *
   * @param profileName broker profile name
   */
  public void unsubscribe(final String profileName) {
    executionBoundController.execute(
        () -> {
          amqpClient.unsubscribe(profileName);
          return null;
        });
  }

  /**
   * Creates a FlowFile containing the AMQP message body and mapped metadata.
   *
   * @param session NiFi process session used to create and write the FlowFile
   * @param consumedMessage received AMQP message
   * @return FlowFile ready for transfer
   */
  public FlowFile toFlowFile(final ProcessSession session, final ConsumedMessage consumedMessage) {
    FlowFile flowFile = session.create();
    flowFile = session.write(flowFile, outputStream -> outputStream.write(consumedMessage.body()));
    flowFile = session.putAllAttributes(flowFile, consumedMessage.attributes());
    return flowFile;
  }
}
