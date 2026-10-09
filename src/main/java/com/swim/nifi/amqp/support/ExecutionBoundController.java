package com.swim.nifi.amqp.support;

import com.swim.nifi.amqp.config.AmqpExtensionProperties;
import com.swim.nifi.amqp.exception.RetryableAmqpException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;

/**
 * Concurrency guard that bounds in-flight AMQP work while running the actual broker interactions on
 * virtual threads.
 */
public final class ExecutionBoundController {

  private final Semaphore semaphore;
  private final Executor executor;

  /**
   * Creates a new concurrency controller.
   *
   * @param properties extension configuration properties providing the concurrency limit
   * @param executor executor used to run broker tasks
   */
  public ExecutionBoundController(
      final AmqpExtensionProperties properties, final Executor executor) {
    this.semaphore = new Semaphore(properties.getDefaults().getMaxConcurrentOperations());
    this.executor = executor;
  }

  /**
   * Executes the supplied broker operation under the configured concurrency limit.
   *
   * @param callable broker operation to run
   * @param <T> result type
   * @return operation result
   */
  public <T> T execute(final Callable<T> callable) {
    try {
      semaphore.acquire();
      final FutureTask<T> futureTask = new FutureTask<>(callable);
      executor.execute(futureTask);
      return futureTask.get();
    } catch (final InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new RetryableAmqpException("Interrupted while waiting for execution permit", exception);
    } catch (final ExecutionException exception) {
      final Throwable cause = exception.getCause();
      if (cause instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      throw new RetryableAmqpException("Broker operation failed", cause);
    } finally {
      semaphore.release();
    }
  }
}
