package com.swim.nifi.amqp.runtime;

import com.swim.nifi.amqp.client.AmqpClient;
import com.swim.nifi.amqp.handler.ConsumeMessageHandler;
import com.swim.nifi.amqp.handler.PublishMessageHandler;
import java.util.concurrent.ExecutorService;

/** Plain runtime implementation that holds the handler graph and owns transport resources. */
public class StandardProcessorRuntime implements ProcessorRuntime {

  private final PublishMessageHandler publishMessageHandler;
  private final ConsumeMessageHandler consumeMessageHandler;
  private final AmqpClient amqpClient;
  private final ExecutorService executorService;

  /**
   * Creates a processor runtime backed by manually assembled handler and transport components.
   *
   * @param publishMessageHandler publish handler
   * @param consumeMessageHandler consume handler
   * @param amqpClient AMQP transport facade
   * @param executorService executor used for bounded broker interactions
   */
  public StandardProcessorRuntime(
      final PublishMessageHandler publishMessageHandler,
      final ConsumeMessageHandler consumeMessageHandler,
      final AmqpClient amqpClient,
      final ExecutorService executorService) {
    this.publishMessageHandler = publishMessageHandler;
    this.consumeMessageHandler = consumeMessageHandler;
    this.amqpClient = amqpClient;
    this.executorService = executorService;
  }

  @Override
  public PublishMessageHandler publishHandler() {
    return publishMessageHandler;
  }

  @Override
  public ConsumeMessageHandler consumeHandler() {
    return consumeMessageHandler;
  }

  @Override
  public void close() {
    try {
      amqpClient.close();
    } finally {
      executorService.close();
    }
  }
}
