package com.swim.nifi.amqp.client;

import java.io.Closeable;
import org.apache.qpid.protonj2.client.Connection;

/**
 * A resource wrapper that bundles an AMQP client connection together with its associated worker
 * instance (such as a consumer or publisher), implementing {@link Closeable} to ensure proper
 * resource cleanup.
 *
 * @param <T> the type of the worker, extending {@link AmqpWorker}
 * @param connection the underlying AMQP connection shared or utilized by the worker
 * @param worker the active worker instance managing publishing or consuming tasks
 */
public record AmqpResource<T extends AmqpWorker>(Connection connection, T worker)
    implements Closeable {

  @Override
  public void close() {
    worker.close();
  }
}
