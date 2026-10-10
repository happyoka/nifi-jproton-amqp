package com.swim.nifi.amqp.client;

import com.swim.nifi.amqp.mapping.FlowFileAttributeMapper;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

/**
 * Base class for implementing publishing and consuming AMQP workers.
 *
 * @see AmqpPublisher
 * @see AmqpConsumer
 */
public class AmqpWorker implements AutoCloseable {
  /**
   * The utility mapper responsible for converting Apache NiFi FlowFiles and their associated
   * attributes into corresponding AMQP messages.
   */
  protected final FlowFileAttributeMapper mapper =
      new FlowFileAttributeMapper(new com.fasterxml.jackson.databind.ObjectMapper());

  /**
   * The component logger used to record lifecycle events, operational status, warnings, and error
   * diagnostics for the processor.
   */
  protected final ComponentLog processorLog;

  /**
   * The active Apache NiFi session managing FlowFile operations, content modifications, and
   * relationship transfers.
   */
  protected final Session session;

  /**
   * Creates an instance of this worker initializing it with AMQP {@link Connection} and creating a
   * target {@link Session} used by subclasses to interact with AMQP-based messaging system.
   *
   * @param connection instance of {@link Connection}
   * @param processorLog instance of {@link ComponentLog} used for logging
   */
  public AmqpWorker(final Connection connection, ComponentLog processorLog) {
    this.processorLog = processorLog;
    validateConnection(connection);
    try {
      this.session = connection.openSession();
    } catch (ClientException e) {
      processorLog.error("Failed to create session for {}", connection, e);
      throw new IllegalStateException(e);
    }
  }

  /**
   * Safely converts an object to its string representation, returning {@code null} if the provided
   * value is null.
   *
   * @param value the object to convert to a string
   * @return the string representation of the object, or {@code null} if the input is null
   */
  public static String stringValue(final Object value) {
    return value == null ? null : String.valueOf(value);
  }

  /**
   * Retrieves a time period property from the given Apache NiFi process context as a {@link
   * Duration}, returning a specified fallback value if the property is unconfigured or null.
   *
   * @param context the NiFi process context containing the component properties
   * @param descriptor the property descriptor identifying the time period property to read
   * @param fallback the default duration to return if the property value is not set
   * @return the parsed {@link Duration} from the property, or the fallback duration if null
   */
  public static Duration duration(
      final ProcessContext context, final PropertyDescriptor descriptor, final Duration fallback) {
    final Long millis = context.getProperty(descriptor).asTimePeriod(TimeUnit.MILLISECONDS);
    return millis == null ? fallback : Duration.ofMillis(millis);
  }

  /**
   * Validates that {@link Connection} is not null and open.
   *
   * @param connection instance of {@link Connection}
   */
  private void validateConnection(Connection connection) {
    if (connection == null) {
      throw new IllegalArgumentException("Connection must not be null!");
    }
    if (connection.openFuture().isDone() && !connection.openFuture().isCancelled()) {
      throw new IllegalStateException("Connection must be open");
    }
  }

  /** Hook for subclasses to stop any long-lived consumer state before the runtime is closed. */
  protected void onProcessorStopped() {}

  @Override
  public void close() throws RuntimeException {
    onProcessorStopped();
    if (session != null) {
      this.session.close();
    }
  }
}
