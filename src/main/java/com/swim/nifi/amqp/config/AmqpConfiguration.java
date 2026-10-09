package com.swim.nifi.amqp.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.swim.nifi.amqp.client.AmqpClient;
import com.swim.nifi.amqp.client.ProtonAmqpClient;
import com.swim.nifi.amqp.handler.ConsumeMessageHandler;
import com.swim.nifi.amqp.handler.PublishMessageHandler;
import com.swim.nifi.amqp.mapping.FlowFileAttributeMapper;
import com.swim.nifi.amqp.support.ExecutionBoundController;
import com.swim.nifi.amqp.support.PayloadGenerator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plain-Java assembler that creates the AMQP transport, mapping helpers, and message handlers used
 * by the NiFi processors.
 */
public class AmqpConfiguration {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpConfiguration.class);

  /**
   * Creates the shared virtual-thread executor used by message handlers to isolate broker work from
   * NiFi callback threads while keeping per-operation overhead low.
   *
   * @return executor service backed by Java virtual threads
   */
  public ExecutorService amqpExecutorService() {
    LOGGER.info("Creating AMQP virtual-thread executor");
    return Executors.newVirtualThreadPerTaskExecutor();
  }

  /**
   * Creates the rate and concurrency controller from processor-supplied defaults.
   *
   * @param properties extension configuration properties
   * @param executorService executor used to run bounded work on virtual threads
   * @return execution controller bound to configured limits
   */
  public ExecutionBoundController executionBoundController(
      final AmqpExtensionProperties properties, final ExecutorService executorService) {
    return new ExecutionBoundController(properties, executorService);
  }

  /**
   * Creates the AMQP client facade responsible for ProtonJ2 connection and link management.
   *
   * @param properties extension configuration properties
   * @return client facade for publish and consume operations
   */
  public AmqpClient amqpClient(final AmqpExtensionProperties properties) {
    return new ProtonAmqpClient(properties);
  }

  /**
   * Creates the FlowFile-to-AMQP metadata mapper.
   *
   * @return metadata mapping helper
   */
  public FlowFileAttributeMapper flowFileAttributeMapper() {
    return new FlowFileAttributeMapper(new ObjectMapper());
  }

  /**
   * Creates the payload helper used to materialize NiFi content for outbound AMQP messages.
   *
   * @return payload generator
   */
  public PayloadGenerator payloadGenerator() {
    return new PayloadGenerator();
  }

  /**
   * Creates the publish handler.
   *
   * @param executionBoundController concurrency controller
   * @param amqpClient AMQP transport facade
   * @param flowFileAttributeMapper metadata mapper
   * @param payloadGenerator payload generator
   * @return publish handler
   */
  public PublishMessageHandler publishMessageHandler(
      final ExecutionBoundController executionBoundController,
      final AmqpClient amqpClient,
      final FlowFileAttributeMapper flowFileAttributeMapper,
      final PayloadGenerator payloadGenerator) {
    return new PublishMessageHandler(
        executionBoundController, amqpClient, flowFileAttributeMapper, payloadGenerator);
  }

  /**
   * Creates the consume handler.
   *
   * @param executionBoundController concurrency controller
   * @param amqpClient AMQP transport facade
   * @return consume handler
   */
  public ConsumeMessageHandler consumeMessageHandler(
      final ExecutionBoundController executionBoundController, final AmqpClient amqpClient) {
    return new ConsumeMessageHandler(executionBoundController, amqpClient);
  }
}
