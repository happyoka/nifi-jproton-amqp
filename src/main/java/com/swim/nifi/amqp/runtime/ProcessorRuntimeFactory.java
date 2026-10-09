package com.swim.nifi.amqp.runtime;

import com.swim.nifi.amqp.config.AmqpExtensionProperties;

/** Factory for creating a processor-local runtime. */
public interface ProcessorRuntimeFactory {
  /**
   * Creates a runtime configured from processor-supplied AMQP properties.
   *
   * @param properties resolved AMQP settings for the processor instance
   * @return initialized processor runtime
   */
  ProcessorRuntime create(AmqpExtensionProperties properties);
}
