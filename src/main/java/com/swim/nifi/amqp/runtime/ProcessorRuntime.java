package com.swim.nifi.amqp.runtime;

import com.swim.nifi.amqp.handler.ConsumeMessageHandler;
import com.swim.nifi.amqp.handler.PublishMessageHandler;

/** Runtime view exposed to NiFi processors after the processor-local AMQP runtime is created. */
public interface ProcessorRuntime extends AutoCloseable {

  /**
   * Returns the publish handler.
   *
   * @return publish handler bean
   */
  PublishMessageHandler publishHandler();

  /**
   * Returns the consume handler.
   *
   * @return consume handler bean
   */
  ConsumeMessageHandler consumeHandler();

  /** Closes processor-local resources. */
  @Override
  void close();
}
