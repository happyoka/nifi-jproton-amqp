package com.swim.nifi.amqp.processor;

import com.swim.nifi.amqp.exception.RetryableAmqpException;
import com.swim.nifi.amqp.runtime.ProcessorRuntime;
import com.swim.nifi.amqp.runtime.ProcessorRuntimeFactory;
import com.swim.nifi.amqp.runtime.StandardProcessorRuntimeFactory;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;

/**
 * Common NiFi processor support for the AMQP processors.
 *
 * <p>The base class owns the lifecycle of a processor-local runtime so each processor instance gets
 * isolated connection configuration, handler state, and transport resources.
 */
public abstract class BaseAmqpProcessor extends AbstractProcessor {
  protected static final String DEFAULT_PROFILE = AmqpProcessorPropertySupport.DEFAULT_PROFILE;

  /** AMQP address configured directly on the processor. */
  protected static final PropertyDescriptor ADDRESS = AmqpProcessorPropertySupport.ADDRESS;

  /** AMQP host configured directly on the processor. */
  protected static final PropertyDescriptor HOST = AmqpProcessorPropertySupport.HOST;

  /** AMQP port configured directly on the processor. */
  protected static final PropertyDescriptor PORT = AmqpProcessorPropertySupport.PORT;

  /** Relationship for successfully processed messages or FlowFiles. */
  protected static final Relationship REL_SUCCESS =
      new Relationship.Builder()
          .name("success")
          .description("Successfully processed messages or FlowFiles.")
          .build();

  /** Relationship for non-retryable mapping, parsing, or broker failures. */
  protected static final Relationship REL_FAILURE =
      new Relationship.Builder()
          .name("failure")
          .description(
              "Messages or FlowFiles that failed due to malformed content or non-retryable faults.")
          .build();

  /** Relationship for retryable network, broker, or transient repository failures. */
  protected static final Relationship REL_RETRY =
      new Relationship.Builder()
          .name("retry")
          .description(
              "Messages or FlowFiles that should be retried because the fault appears transient.")
          .build();

  private final ProcessorRuntimeFactory runtimeFactory;
  private final AtomicReference<ProcessorRuntime> runtimeReference = new AtomicReference<>();

  /** Creates a processor using the default NiFi-style runtime factory. */
  protected BaseAmqpProcessor() {
    this(new StandardProcessorRuntimeFactory());
  }

  /**
   * Creates a processor using a caller-supplied runtime factory, primarily for deterministic unit
   * testing.
   *
   * @param runtimeFactory runtime factory that creates processor-local handler infrastructure
   */
  protected BaseAmqpProcessor(final ProcessorRuntimeFactory runtimeFactory) {
    this.runtimeFactory = runtimeFactory;
  }

  /**
   * Returns the common property descriptors used by both processors.
   *
   * @return common property descriptors
   */
  protected List<PropertyDescriptor> commonProperties() {
    return AmqpProcessorPropertySupport.descriptors();
  }

  /**
   * Starts a fresh runtime for the processor instance whenever NiFi schedules the processor.
   *
   * @param context processor context containing current property values
   */
  @OnScheduled
  public final void onScheduled(final ProcessContext context) {
    getLogger().info("{} scheduled - initializing AMQP runtime", getClass().getSimpleName());
    closeRuntime();
    try {
      runtimeReference.set(
          runtimeFactory.create(AmqpProcessorPropertySupport.toProperties(context)));
      onProcessorScheduled(context);
      getLogger().info("{} runtime created successfully", getClass().getSimpleName());
    } catch (final Exception exception) {
      getLogger().error("Failed to initialize AMQP runtime during scheduling", exception);
      throw exception;
    }
  }

  /** Closes processor-local runtime resources when NiFi stops the processor. */
  @OnStopped
  public final void onStopped() {
    onProcessorStopped();
    closeRuntime();
  }

  /**
   * Hook for subclasses to start any long-lived consumer state once the runtime is ready.
   *
   * @param context processor context used to access current property values
   */
  protected void onProcessorScheduled(final ProcessContext context) {}

  /** Hook for subclasses to stop any long-lived consumer state before the runtime is closed. */
  protected void onProcessorStopped() {}

  /**
   * Returns the active runtime, starting one lazily if a test invokes the processor without a
   * schedule callback.
   *
   * @param context current processor context
   * @return active runtime
   */
  protected final ProcessorRuntime runtime(final ProcessContext context) {
    ProcessorRuntime runtime = runtimeReference.get();
    if (runtime == null) {
      runtime = runtimeFactory.create(AmqpProcessorPropertySupport.toProperties(context));
      if (!runtimeReference.compareAndSet(null, runtime)) {
        runtime.close();
        runtime = runtimeReference.get();
      }
    }
    return runtime;
  }

  /**
   * Returns the currently initialized runtime without creating a new one.
   *
   * @return current runtime or {@code null} when none is active
   */
  protected final ProcessorRuntime runtimeIfPresent() {
    return runtimeReference.get();
  }

  /**
   * Returns the standard relationships supported by both processors.
   *
   * @return success, failure, and retry relationships
   */
  protected final Set<Relationship> commonRelationships() {
    return Set.of(REL_SUCCESS, REL_FAILURE, REL_RETRY);
  }

  /**
   * Returns whether the retryable exception represents an AMQP transport failure that should be
   * surfaced as a clearer processor bulletin.
   *
   * @param exception retryable AMQP exception
   * @return {@code true} when the error came from AMQP connect, publish, subscribe, or poll
   *     operations
   */
  protected final boolean isBrokerTransportFailure(final RetryableAmqpException exception) {
    final String message = exception.getMessage();
    return message != null
        && (message.startsWith("Failed to open AMQP connection")
            || message.startsWith("Failed to publish AMQP message to ")
            || message.startsWith("Failed to subscribe to AMQP message source ")
            || message.startsWith("Failed to poll AMQP subscription"));
  }

  /**
   * Returns the configured broker host and port for operator-facing messages.
   *
   * @param context processor context
   * @return broker endpoint in host:port format
   */
  protected final String brokerEndpoint(final ProcessContext context) {
    return context.getProperty(HOST).getValue() + ":" + context.getProperty(PORT).getValue();
  }

  /**
   * Formats a clearer operator-facing message for AMQP transport failures that are routed to retry.
   *
   * @param operation human-readable action such as publishing or consuming
   * @param endpoint configured broker endpoint
   * @param address configured AMQP address
   * @param exception retryable AMQP exception
   * @return formatted retry bulletin message
   */
  protected final String transportFailureMessage(
      final String operation,
      final String endpoint,
      final String address,
      final RetryableAmqpException exception) {
    final String detail = transportFailureDetail(exception);
    return String.format(
        "Transient AMQP transport failure while %s with broker %s at %s; routing to retry. Cause: %s",
        operation, endpoint, address, detail);
  }

  private String transportFailureDetail(final RetryableAmqpException exception) {
    Throwable current = exception;
    String detail = exception.getMessage();
    while (current != null) {
      if (current.getMessage() != null && !current.getMessage().isBlank()) {
        detail = current.getMessage();
      }
      current = current.getCause();
    }
    return detail == null || detail.isBlank() ? "Unknown transport error" : detail;
  }

  private void closeRuntime() {
    final ProcessorRuntime runtime = runtimeReference.getAndSet(null);
    if (runtime != null) {
      runtime.close();
    }
  }
}
