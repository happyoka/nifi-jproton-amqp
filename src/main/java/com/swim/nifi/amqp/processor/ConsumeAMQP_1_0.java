package com.swim.nifi.amqp.processor;

import com.swim.nifi.amqp.exception.RetryableAmqpException;
import com.swim.nifi.amqp.exception.UnparseableAmqpMessageException;
import com.swim.nifi.amqp.handler.ConsumeMessageHandler;
import com.swim.nifi.amqp.model.ConsumedMessage;
import com.swim.nifi.amqp.runtime.ProcessorRuntime;
import com.swim.nifi.amqp.runtime.ProcessorRuntimeFactory;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;

/** Consumes AMQP 1.0 messages from a configured node and emits them as NiFi FlowFiles. */
@Tags({"amqp", "amqp1.0", "consume", "qpid", "proton"})
@CapabilityDescription(
    "Consumes AMQP 1.0 messages from a configured queue, topic, or node and maps message metadata to FlowFile attributes.")
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
  @WritesAttribute(
      attribute = "amqp.message-id",
      description = "Copied from the inbound AMQP message-id field when present."),
  @WritesAttribute(
      attribute = "amqp.correlation-id",
      description = "Copied from the inbound AMQP correlation-id field when present."),
  @WritesAttribute(
      attribute = "amqp.subject",
      description = "Copied from the inbound AMQP subject field when present."),
  @WritesAttribute(
      attribute = "amqp.header.*",
      description = "Copied from supported AMQP header fields."),
  @WritesAttribute(
      attribute = "amqp.application.*",
      description = "Copied from AMQP application properties."),
  @WritesAttribute(
      attribute = "amqp.delivery-annotation.*",
      description = "Copied from AMQP delivery annotations when present."),
  @WritesAttribute(
      attribute = "traceparent",
      description = "Copied from AMQP application properties for distributed tracing."),
  @WritesAttribute(
      attribute = "tracestate",
      description = "Copied from AMQP application properties for distributed tracing."),
  @WritesAttribute(
      attribute = "baggage",
      description = "Copied from AMQP application properties for distributed tracing.")
})
public final class ConsumeAMQP_1_0 extends BaseAmqpProcessor {
  private static final long MAX_SUBSCRIPTION_RECOVERY_BACKOFF_MILLIS = 5_000L;

  private final Object subscriptionMonitor = new Object();
  private final AtomicReference<String> subscribedAddress = new AtomicReference<>();
  private final AtomicBoolean subscriptionActive = new AtomicBoolean(false);
  private final AtomicLong nextSubscriptionAttemptAtMillis = new AtomicLong();
  private final AtomicReference<String> lastSubscriptionFailure = new AtomicReference<>();
  private List<PropertyDescriptor> descriptors;
  private Set<Relationship> relationships;

  /** Creates a processor using the default processor property-backed runtime factory. */
  public ConsumeAMQP_1_0() {
    super();
  }

  /**
   * Creates a processor using a supplied runtime factory.
   *
   * @param runtimeFactory runtime factory used to provision handlers for the processor instance
   */
  ConsumeAMQP_1_0(final ProcessorRuntimeFactory runtimeFactory) {
    super(runtimeFactory);
  }

  /**
   * Initializes processor descriptors and relationships.
   *
   * @param context initialization context provided by NiFi
   */
  @Override
  protected void init(final ProcessorInitializationContext context) {
    descriptors = commonProperties();
    relationships = commonRelationships();
  }

  @Override
  protected void onProcessorScheduled(final ProcessContext context) {
    nextSubscriptionAttemptAtMillis.set(0L);
    lastSubscriptionFailure.set(null);
    ensureSubscription(context);
  }

  @Override
  protected void onProcessorStopped() {
    closeSubscription();
  }

  /**
   * Returns the property descriptors supported by this processor.
   *
   * @return supported descriptors
   */
  @Override
  public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return descriptors;
  }

  /**
   * Returns the relationships supported by this processor.
   *
   * @return supported relationships
   */
  @Override
  public Set<Relationship> getRelationships() {
    return relationships;
  }

  /**
   * Drains the local subscription buffer and emits FlowFiles. Subscription startup happens during
   * processor scheduling, so onTrigger only needs to drain messages and recover if the subscription
   * died.
   *
   * @param context processor context
   * @param session NiFi process session
   */
  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) {
    getLogger().debug("ConsumeAMQP onTrigger called");
    final String address = context.getProperty(ADDRESS).getValue();
    final ConsumeMessageHandler consumeHandler = runtime(context).consumeHandler();

    if (!subscriptionActive.get()) {
      try {
        if (!ensureSubscription(context)) {
          context.yield();
          return;
        }
      } catch (final RetryableAmqpException exception) {
        handleSubscriptionFailure(context, session, address, exception);
        return;
      } catch (final Exception exception) {
        getLogger()
            .error(
                "Failed to establish AMQP subscription to {}: {}",
                address,
                exception.getMessage(),
                exception);
        final FlowFile flowFile =
            session.putAttribute(session.create(), "amqp.error", exception.getMessage());
        session.transfer(flowFile, REL_FAILURE);
        return;
      }
    }

    final ConsumedMessage consumedMessage;
    try {
      consumedMessage = consumeHandler.poll(DEFAULT_PROFILE).orElse(null);
    } catch (final RetryableAmqpException exception) {
      closeSubscription();
      handleSubscriptionFailure(context, session, address, exception);
      return;
    } catch (final UnparseableAmqpMessageException exception) {
      getLogger()
          .error("Inbound AMQP message was malformed: {}", exception.getMessage(), exception);
      final FlowFile flowFile =
          session.putAttribute(session.create(), "amqp.error", exception.getMessage());
      session.transfer(flowFile, REL_FAILURE);
      return;
    } catch (final Exception exception) {
      closeSubscription();
      getLogger()
          .error(
              "Unexpected error polling AMQP subscription: {}", exception.getMessage(), exception);
      final FlowFile flowFile =
          session.putAttribute(session.create(), "amqp.error", exception.getMessage());
      session.transfer(flowFile, REL_RETRY);
      context.yield();
      return;
    }

    if (consumedMessage == null) {
      context.yield();
      return;
    }

    final FlowFile flowFile = consumeHandler.toFlowFile(session, consumedMessage);
    session.transfer(flowFile, REL_SUCCESS);
    session.commitAsync(
        () -> consumedMessage.acknowledgement().settle(consumedMessage.successSettlement()),
        throwable -> consumedMessage.acknowledgement().settle(consumedMessage.retrySettlement()));
  }

  private boolean ensureSubscription(final ProcessContext context) {
    if (subscriptionActive.get()) {
      return true;
    }

    final long now = System.currentTimeMillis();
    if (now < nextSubscriptionAttemptAtMillis.get()) {
      getLogger()
          .debug(
              "ConsumeAMQP subscription recovery backoff is active for address {}: {}",
              context.getProperty(ADDRESS).getValue(),
              lastSubscriptionFailure.get());
      return false;
    }

    synchronized (subscriptionMonitor) {
      if (subscriptionActive.get()) {
        return true;
      }

      final long retryNow = System.currentTimeMillis();
      if (retryNow < nextSubscriptionAttemptAtMillis.get()) {
        return false;
      }

      final String address = context.getProperty(ADDRESS).getValue();
      runtime(context).consumeHandler().subscribe(DEFAULT_PROFILE, address);
      subscribedAddress.set(address);
      subscriptionActive.set(true);
      nextSubscriptionAttemptAtMillis.set(0L);
      lastSubscriptionFailure.set(null);
      getLogger().info("ConsumeAMQP subscription established for address: {}", address);
      return true;
    }
  }

  private void closeSubscription() {
    synchronized (subscriptionMonitor) {
      final ProcessorRuntime runtime = runtimeIfPresent();
      final String address = subscribedAddress.getAndSet(null);
      final boolean wasActive = subscriptionActive.getAndSet(false);
      if (!wasActive || runtime == null) {
        return;
      }

      try {
        runtime.consumeHandler().unsubscribe(DEFAULT_PROFILE);
        getLogger().info("ConsumeAMQP subscription closed for address: {}", address);
      } catch (final Exception exception) {
        getLogger()
            .warn("Error closing ConsumeAMQP subscription: {}", exception.getMessage(), exception);
      }
    }
  }

  private void handleSubscriptionFailure(
      final ProcessContext context,
      final ProcessSession session,
      final String address,
      final RetryableAmqpException exception) {
    markSubscriptionRecoveryBackoff(context, exception);
    final String retryMessage;
    if (isBrokerTransportFailure(exception)) {
      retryMessage =
          transportFailureMessage("consuming", brokerEndpoint(context), address, exception);
      getLogger().error(retryMessage, exception);
    } else {
      retryMessage = exception.getMessage();
      getLogger().warn("Retryable AMQP subscription failure due to {}", retryMessage, exception);
    }
    context.yield();
    final FlowFile flowFile = session.putAttribute(session.create(), "amqp.error", retryMessage);
    session.transfer(flowFile, REL_RETRY);
  }

  private void markSubscriptionRecoveryBackoff(
      final ProcessContext context, final RetryableAmqpException exception) {
    lastSubscriptionFailure.set(exception.getMessage());
    final Long configuredTimeoutMillis =
        context
            .getProperty(AmqpProcessorPropertySupport.RECEIVE_TIMEOUT)
            .asTimePeriod(TimeUnit.MILLISECONDS);
    final long backoffMillis =
        Math.min(
            MAX_SUBSCRIPTION_RECOVERY_BACKOFF_MILLIS,
            Math.max(250L, configuredTimeoutMillis == null ? 1_000L : configuredTimeoutMillis));
    nextSubscriptionAttemptAtMillis.set(System.currentTimeMillis() + backoffMillis);
  }
}
