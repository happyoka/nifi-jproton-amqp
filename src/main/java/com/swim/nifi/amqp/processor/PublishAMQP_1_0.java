package com.swim.nifi.amqp.processor;

import com.swim.nifi.amqp.exception.RetryableAmqpException;
import com.swim.nifi.amqp.exception.UnparseableAmqpMessageException;
import com.swim.nifi.amqp.runtime.ProcessorRuntimeFactory;
import java.util.List;
import java.util.Set;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

/** Publishes incoming NiFi FlowFile payloads as AMQP 1.0 messages using Apache Qpid ProtonJ2. */
@Tags({"amqp", "amqp1.0", "publish", "qpid", "proton"})
@CapabilityDescription(
    "Publishes FlowFile content to an AMQP 1.0 node and maps FlowFile attributes to AMQP headers and application properties.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@ReadsAttributes({
  @ReadsAttribute(
      attribute = "amqp.message-id",
      description = "Maps to the AMQP message-id field when present."),
  @ReadsAttribute(
      attribute = "amqp.correlation-id",
      description = "Maps to the AMQP correlation-id field when present."),
  @ReadsAttribute(
      attribute = "amqp.subject",
      description = "Maps to the AMQP subject field when present."),
  @ReadsAttribute(
      attribute = "amqp.header.*",
      description = "Mapped to standard AMQP header fields when supported."),
  @ReadsAttribute(
      attribute = "amqp.application.*",
      description = "Mapped to AMQP application properties."),
  @ReadsAttribute(
      attribute = "traceparent",
      description = "Forwarded as an AMQP application property for distributed tracing."),
  @ReadsAttribute(
      attribute = "tracestate",
      description = "Forwarded as an AMQP application property for distributed tracing."),
  @ReadsAttribute(
      attribute = "baggage",
      description = "Forwarded as an AMQP application property for distributed tracing.")
})
public final class PublishAMQP_1_0 extends BaseAmqpProcessor {
  private List<PropertyDescriptor> descriptors;
  private Set<Relationship> relationships;

  /** Creates a processor using the default processor property-backed runtime factory. */
  public PublishAMQP_1_0() {
    super();
  }

  /**
   * Creates a processor using a supplied runtime factory.
   *
   * @param runtimeFactory runtime factory used to provision handlers for the processor instance
   */
  PublishAMQP_1_0(final ProcessorRuntimeFactory runtimeFactory) {
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
   * Publishes one incoming FlowFile to the configured AMQP target and routes the FlowFile based on
   * remote settlement or fault classification.
   *
   * @param context processor context
   * @param session NiFi process session
   * @throws ProcessException when a framework-level error prevents routing
   */
  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) {
    final FlowFile flowFile = session.get();
    if (flowFile == null) {
      return;
    }
    try {
      runtime(context)
          .publishHandler()
          .publish(session, flowFile, DEFAULT_PROFILE, context.getProperty(ADDRESS).getValue());
      session.transfer(flowFile, REL_SUCCESS);
    } catch (final RetryableAmqpException exception) {
      if (isBrokerTransportFailure(exception)) {
        getLogger()
            .error(
                transportFailureMessage(
                    "publishing",
                    brokerEndpoint(context),
                    context.getProperty(ADDRESS).getValue(),
                    exception),
                exception);
      } else {
        getLogger()
            .warn(
                "Retryable AMQP publish failure for FlowFile {} due to {}",
                flowFile,
                exception.getMessage(),
                exception);
      }
      session.penalize(flowFile);
      session.transfer(flowFile, REL_RETRY);
    } catch (final UnparseableAmqpMessageException exception) {
      getLogger()
          .error(
              "Non-retryable AMQP publish failure for FlowFile {} due to {}",
              flowFile,
              exception.getMessage(),
              exception);
      session.transfer(flowFile, REL_FAILURE);
    } catch (final Exception exception) {
      getLogger()
          .error(
              "Unexpected publish failure for FlowFile {} due to {}",
              flowFile,
              exception.getMessage(),
              exception);
      session.penalize(flowFile);
      session.transfer(flowFile, REL_RETRY);
    }
  }
}
