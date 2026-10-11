package com.swim.nifi.amqp.processor;

import static com.swim.nifi.amqp.client.AmqpWorker.duration;

import com.swim.nifi.amqp.client.AmqpPublisher;
import com.swim.nifi.amqp.exception.RetryableAmqpException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

/** Publishes incoming NiFi FlowFile payloads as AMQP 1.0 messages using Apache Qpid ProtonJ2. */
@Tags({"amqp", "amqp1.0", "publish", "qpid", "proton"})
@CapabilityDescription(
    "Publishes FlowFile content to an AMQP 1.0 node and maps FlowFile attributes to AMQP headers and application properties.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
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
})
public class PublishAmqp10 extends AbstractAmqpProcessor<AmqpPublisher> {
  /**
   * A property descriptor that defines the maximum amount of time to wait for a message send or
   * publish operation to complete before timing out.
   */
  static final PropertyDescriptor SEND_TIMEOUT =
      new PropertyDescriptor.Builder()
          .name("Send Timeout")
          .description("Maximum wait for remote settlement when publishing.")
          .required(true)
          .defaultValue("15 sec")
          .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
          .build();

  /**
   * Property descriptor defining whether the messaging destination functions as a Queue
   * (point-to-point / anycast) or a Topic (publish-subscribe / multicast).
   *
   * <p>This governs how the client and broker route messages—directing payloads to a single
   * consumer queue or broadcasting them across multiple subscriptions depending on the underlying
   * messaging implementation.
   */
  static final PropertyDescriptor DESTINATION_TYPE =
      new PropertyDescriptor.Builder()
          .name("Destination Type")
          .description("The type of destination to use (either a Queue or a Topic).")
          .required(true)
          .allowableValues("QUEUE", "TOPIC")
          .defaultValue("TOPIC")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  /**
   * The relationship representing FlowFiles that have been successfully processed, produced, or
   * consumed and routed onward in the NiFi data flow.
   */
  static final Relationship REL_SUCCESS =
      new Relationship.Builder()
          .name("success")
          .description(
              "All FlowFiles that are sent to the AMQP destination are routed to this relationship")
          .build();

  /**
   * The relationship representing FlowFiles that failed to be processed, produced, or consumed and
   * cannot be routed to success or retry paths.
   */
  static final Relationship REL_FAILURE =
      new Relationship.Builder()
          .name("failure")
          .description(
              "All FlowFiles that cannot be routed to the AMQP destination are routed to this relationship")
          .build();

  static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);

  /**
   * The maximum allowable size limit for an input FlowFile payload (set to 10 MB), used as a
   * default or validation boundary for incoming data streams.
   */
  private static final long MAXIMUM_INPUT_FLOWFILE_SIZE_LIMIT = 10 * 1024 * 1024L;

  /**
   * A property descriptor that defines the maximum allowable size of an input FlowFile payload
   * permitted for processing, preventing excessively large messages from overwhelming system memory
   * or resources.
   */
  static final PropertyDescriptor MAXIMUM_INPUT_FLOWFILE_SIZE =
      new PropertyDescriptor.Builder()
          .name("Maximum Input FlowFile Size")
          .description(
              """
                    Maximum size of an input FlowFile that will be read into memory before publishing. PublishAMQP reads FlowFile content into a byte array
                    before publishing, so FlowFiles larger than this value are routed to failure before content is read. Configure this value according to
                    broker limits and available JVM memory.
                    """)
          .required(true)
          .defaultValue("10 MB")
          .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
          .addValidator(
              StandardValidators.createDataSizeBoundsValidator(
                  1, MAXIMUM_INPUT_FLOWFILE_SIZE_LIMIT))
          .build();

  static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS =
      Stream.concat(
              getCommonPropertyDescriptors().stream(),
              Stream.of(DESTINATION_TYPE, SEND_TIMEOUT, MAXIMUM_INPUT_FLOWFILE_SIZE))
          .toList();

  /** Constructs a new PublishAmqp10 processor instance. */
  public PublishAmqp10() {
    super();
  }

  @Override
  protected void processResource(
      Connection connection,
      AmqpPublisher publisher,
      ProcessContext context,
      ProcessSession session)
      throws ProcessException {
    var flowFile = session.get();
    if (flowFile == null) {
      return;
    }
    final long maximumInputFlowFileSize =
        context
            .getProperty(MAXIMUM_INPUT_FLOWFILE_SIZE)
            .evaluateAttributeExpressions()
            .asDataSize(DataUnit.B)
            .longValue();
    if (flowFile.getSize() > maximumInputFlowFileSize) {
      getLogger()
          .warn(
              "FlowFile {} with size {} bytes exceeds configured maximum input FlowFile size of {} bytes; routing to failure",
              flowFile,
              flowFile.getSize(),
              maximumInputFlowFileSize);
      session.transfer(flowFile, REL_FAILURE);
    }
    try {
      publisher.publish(flowFile, session);
      session.transfer(flowFile, REL_SUCCESS);
    } catch (RetryableAmqpException e) {
      session.rollback();
      throw e;
    } catch (Exception e) {
      session.transfer(flowFile, REL_FAILURE);
      throw e;
    }
  }

  @Override
  protected AmqpPublisher createAMQPWorker(ProcessContext context, Connection connection) {
    try {
      final String address = context.getProperty(ADDRESS).getValue();
      final String destinationType = context.getProperty(DESTINATION_TYPE).getValue();
      final var sendTimeout = duration(context, SEND_TIMEOUT, Duration.ofSeconds(15)).toMillis();
      return new AmqpPublisher(
          connection, address, destinationType.equalsIgnoreCase("TOPIC"), sendTimeout, getLogger());
    } catch (final ClientException ioe) {
      throw new ProcessException("Failed to connect to AMQP Broker", ioe);
    }
  }

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return PROPERTY_DESCRIPTORS;
  }

  @Override
  public Set<Relationship> getRelationships() {
    return RELATIONSHIPS;
  }
}
