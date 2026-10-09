package com.swim.nifi.amqp.runtime;

import com.swim.nifi.amqp.client.AmqpClient;
import com.swim.nifi.amqp.config.AmqpConfiguration;
import com.swim.nifi.amqp.config.AmqpExtensionProperties;
import com.swim.nifi.amqp.handler.ConsumeMessageHandler;
import com.swim.nifi.amqp.handler.PublishMessageHandler;
import com.swim.nifi.amqp.mapping.FlowFileAttributeMapper;
import com.swim.nifi.amqp.support.ExecutionBoundController;
import com.swim.nifi.amqp.support.PayloadGenerator;
import java.util.concurrent.ExecutorService;

/**
 * Factory for creating a processor-local runtime without Spring Boot, following a plain NiFi-style
 * object graph assembly pattern.
 */
public final class StandardProcessorRuntimeFactory implements ProcessorRuntimeFactory {
  private final AmqpConfiguration configuration;

  /** Creates a runtime factory using the default runtime assembler. */
  public StandardProcessorRuntimeFactory() {
    this(new AmqpConfiguration());
  }

  StandardProcessorRuntimeFactory(final AmqpConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public ProcessorRuntime create(final AmqpExtensionProperties properties) {
    properties.validate();
    final ExecutorService executorService = configuration.amqpExecutorService();
    final FlowFileAttributeMapper mapper = configuration.flowFileAttributeMapper();
    final PayloadGenerator payloadGenerator = configuration.payloadGenerator();
    final ExecutionBoundController executionBoundController =
        configuration.executionBoundController(properties, executorService);
    final AmqpClient amqpClient = configuration.amqpClient(properties);
    final PublishMessageHandler publishMessageHandler =
        configuration.publishMessageHandler(
            executionBoundController, amqpClient, mapper, payloadGenerator);
    final ConsumeMessageHandler consumeMessageHandler =
        configuration.consumeMessageHandler(executionBoundController, amqpClient);
    return new StandardProcessorRuntime(
        publishMessageHandler, consumeMessageHandler, amqpClient, executorService);
  }
}
