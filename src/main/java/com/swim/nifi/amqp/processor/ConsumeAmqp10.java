package com.swim.nifi.amqp.processor;

import static com.swim.nifi.amqp.client.AmqpWorker.duration;

import com.swim.nifi.amqp.client.AmqpConsumer;
import com.swim.nifi.amqp.model.ConsumedMessage;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

/**
 * An Apache NiFi processor that consumes messages from an AMQP 1.0 messaging broker using the Qpid
 * ProtonJ2 client library, transforming received payloads into NiFi FlowFiles and routing them to
 * downstream relationships.
 */
@Tags({"amqp", "amqp1.0", "consume", "qpid", "proton"})
@CapabilityDescription(
    "Consumes AMQP 1.0 messages from a configured queue, topic, or node and maps message metadata to FlowFile attributes.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
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
      description = "Copied from AMQP delivery annotations when present.")
})
public class ConsumeAmqp10 extends AbstractAmqpProcessor<AmqpConsumer> {

  /** A property descriptor defining the credit window behavior used for consuming messages. */
  public static final PropertyDescriptor CREDIT_WINDOW =
      new PropertyDescriptor.Builder()
          .name("Credit Window")
          .description("Receiver credit window. Set to 0 to request one message at a time.")
          .required(true)
          .defaultValue("25")
          .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
          .build();

  /** A property descriptor defining the receive timeout behavior used for consuming messages. */
  public static final PropertyDescriptor RECEIVE_TIMEOUT =
      new PropertyDescriptor.Builder()
          .name("Receive Timeout")
          .description("Maximum wait for a message before yielding.")
          .required(true)
          .defaultValue("5 sec")
          .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
          .build();

  /** A property descriptor defining the auto-acknowledge behavior used for consuming messages. */
  public static final PropertyDescriptor AUTO_ACKNOWLEDGE =
      new PropertyDescriptor.Builder()
          .name("Auto-Acknowledge Messages")
          .description(
              "Whether to automatically acknowledge received messages. "
                  + "True: Messages auto-acknowledged immediately (faster but less safe). "
                  + "False: Manual acknowledgement via settlement (default, safer).")
          .required(true)
          .defaultValue("false")
          .allowableValues("true", "false")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build();

  /**
   * The relationship representing FlowFiles that have been successfully processed, produced, or
   * consumed and routed onward in the NiFi data flow.
   */
  public static final Relationship REL_SUCCESS =
      new Relationship.Builder()
          .name("success")
          .description(
              "All FlowFiles that are received from the AMQP queue are routed to this relationship")
          .build();

  static final PropertyDescriptor BATCH_SIZE =
      new PropertyDescriptor.Builder()
          .name("Batch Size")
          .description(
              "The maximum number of messages that should be processed in a single session. Once this many messages have been received (or once no more messages are readily available), "
                  + "the messages received will be transferred to the 'success' relationship and the messages will be acknowledged to the AMQP Broker. Setting this value to a larger number "
                  + "could result in better performance, particularly for very small messages, but can also result in more messages being duplicated upon sudden restart of NiFi.")
          .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
          .expressionLanguageSupported(ExpressionLanguageScope.NONE)
          .defaultValue("10")
          .required(true)
          .build();
  private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS =
      Stream.concat(
              getCommonPropertyDescriptors().stream(),
              Stream.of(RECEIVE_TIMEOUT, BATCH_SIZE, AUTO_ACKNOWLEDGE, CREDIT_WINDOW))
          .toList();

  private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

  @Override
  protected void processResource(
      final Connection connection,
      final AmqpConsumer consumer,
      final ProcessContext context,
      final ProcessSession session)
      throws ProcessException {
    ConsumedMessage lastReceived = null;
    for (int i = 0; i < context.getProperty(BATCH_SIZE).asInteger(); i++) {
      final var consumedMessage = consumer.poll().orElse(null);
      if (consumedMessage == null) {
        if (lastReceived == null) {
          context.yield();
        }
        break;
      }
      final FlowFile flowFile = consumer.toFlowFile(session, consumedMessage);
      session.transfer(flowFile, REL_SUCCESS);
      session.commitAsync(
          () -> consumedMessage.acknowledgement().settle(consumedMessage.successSettlement()),
          throwable -> consumedMessage.acknowledgement().settle(consumedMessage.retrySettlement()));
      lastReceived = consumedMessage;
    }
  }

  @Override
  protected synchronized AmqpConsumer createAMQPWorker(
      ProcessContext context, Connection connection) {
    try {
      final String address = context.getProperty(ADDRESS).getValue();
      final boolean autoAcknowledge = context.getProperty(AUTO_ACKNOWLEDGE).asBoolean();
      final int creditWindow = context.getProperty(CREDIT_WINDOW).asInteger();
      final var receiveTimeout =
          duration(context, RECEIVE_TIMEOUT, Duration.ofSeconds(15)).toMillis();
      return new AmqpConsumer(
          connection, address, autoAcknowledge, creditWindow, receiveTimeout, getLogger());
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
