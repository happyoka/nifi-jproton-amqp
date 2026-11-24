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
import org.apache.nifi.annotation.lifecycle.OnStopped;
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
      description = "Copied from AMQP delivery annotations when present."),
  @WritesAttribute(
      attribute = "amqp.deferred-ack.token",
      description =
          "Set when deferred acknowledgement is enabled; consumed delivery is acknowledged after downstream publish.")
})
public class ConsumeAmqp10 extends AbstractAmqpProcessor<AmqpConsumer> {

  /** A property descriptor defining the credit window behavior used for consuming messages. */
  static final PropertyDescriptor CREDIT_WINDOW =
      new PropertyDescriptor.Builder()
          .name("Credit Window")
          .description("Receiver credit window. Set to 0 to request one message at a time.")
          .required(true)
          .defaultValue("25")
          .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
          .build();

  /** A property descriptor defining the receive timeout behavior used for consuming messages. */
  static final PropertyDescriptor RECEIVE_TIMEOUT =
      new PropertyDescriptor.Builder()
          .name("Receive Timeout")
          .description("Maximum wait for a message before yielding.")
          .required(true)
          .defaultValue("5 sec")
          .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
          .build();

  /** A property descriptor defining the auto-acknowledge behavior used for consuming messages. */
  static final PropertyDescriptor AUTO_ACKNOWLEDGE =
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

  static final PropertyDescriptor ACKNOWLEDGE_AFTER_PUBLISH =
      new PropertyDescriptor.Builder()
          .name("Acknowledge After Publish")
          .description(
              "When true, received deliveries are not settled during ConsumeAmqp10 commit. "
                  + "Instead, a deferred acknowledgement token is attached and PublishAmqp10 settles the source delivery after send outcome.")
          .required(true)
          .defaultValue("false")
          .allowableValues("true", "false")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build();

  /**
   * The relationship representing FlowFiles that have been successfully processed, produced, or
   * consumed and routed onward in the NiFi data flow.
   */
  static final Relationship REL_SUCCESS =
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
  static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS =
      Stream.concat(
              getCommonPropertyDescriptors().stream(),
              Stream.of(
                  RECEIVE_TIMEOUT,
                  BATCH_SIZE,
                  AUTO_ACKNOWLEDGE,
                  ACKNOWLEDGE_AFTER_PUBLISH,
                  CREDIT_WINDOW))
          .toList();

  static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

  /** Constructs a new ConsumeAmqp10 processor instance. */
  public ConsumeAmqp10() {
    super();
  }

  /**
   * Polls up to {@code BATCH_SIZE} messages from the AMQP consumer, converts each message into a
   * NiFi {@link FlowFile}, routes it to {@code REL_SUCCESS}, and commits the session per message.
   *
   * <p>When {@code ACKNOWLEDGE_AFTER_PUBLISH} is {@code true}, the source delivery is left
   * unsettled upon commit and registered in {@link DeferredAcknowledgementRegistry} with a
   * correlation token stored in {@link
   * DeferredAcknowledgementRegistry#DEFERRED_ACK_TOKEN_ATTRIBUTE} so a downstream publisher can
   * settle it later. If the session commit fails, the registry is invoked to apply the retry
   * settlement immediately.
   *
   * <p>When {@code ACKNOWLEDGE_AFTER_PUBLISH} is {@code false}, the source message is settled
   * directly inside the asynchronous commit callback using the message's configured success or
   * retry settlement mode. If a poll returns empty and no message was received during this run,
   * {@link ProcessContext#yield()} is called before exiting the batch loop.
   *
   * @param connection the active AMQP connection associated with the consumer resource
   * @param consumer the AMQP consumer used to poll messages and create FlowFiles
   * @param context the NiFi process context providing processor properties and yield control
   * @param session the NiFi process session used to create, attribute, route, and commit FlowFiles
   * @throws ProcessException if reading properties, creating/updating FlowFiles, or committing the
   *     session fails
   */
  @Override
  protected void processResource(
      final Connection connection,
      final AmqpConsumer consumer,
      final ProcessContext context,
      final ProcessSession session)
      throws ProcessException {
    final boolean acknowledgeAfterPublish =
        context.getProperty(ACKNOWLEDGE_AFTER_PUBLISH).asBoolean();
    ConsumedMessage lastReceived = null;
    for (int i = 0; i < context.getProperty(BATCH_SIZE).asInteger(); i++) {
      final var consumedMessage = consumer.poll().orElse(null);
      if (consumedMessage == null) {
        final int expiredEntries = DeferredAcknowledgementRegistry.pruneExpiredEntries();
        if (expiredEntries > 0) {
          getLogger().debug("Pruned {} expired deferred acknowledgement entries", expiredEntries);
        }
        if (lastReceived == null) {
          context.yield();
        }
        break;
      }
      if (acknowledgeAfterPublish) {
        FlowFile flowFile = consumer.toFlowFile(session, consumedMessage);
        final String deferredToken = DeferredAcknowledgementRegistry.register(consumedMessage);
        flowFile =
            session.putAttribute(
                flowFile,
                DeferredAcknowledgementRegistry.DEFERRED_ACK_TOKEN_ATTRIBUTE,
                deferredToken);
        session.transfer(flowFile, REL_SUCCESS);
        session.commitAsync(
            () -> {}, throwable -> DeferredAcknowledgementRegistry.settleRetry(deferredToken));
      } else {
        final FlowFile flowFile = consumer.toFlowFile(session, consumedMessage);
        session.transfer(flowFile, REL_SUCCESS);
        session.commitAsync(
            () -> consumedMessage.acknowledgement().settle(consumedMessage.successSettlement()),
            throwable ->
                consumedMessage.acknowledgement().settle(consumedMessage.retrySettlement()));
      }
      lastReceived = consumedMessage;
    }
  }

  @OnStopped
  @Override
  public void close() {
    final int releasedEntries = DeferredAcknowledgementRegistry.settleAllRetry();
    if (releasedEntries > 0) {
      getLogger().info("Released {} deferred acknowledgement entries on stop", releasedEntries);
    }
    super.close();
  }

  /**
   * Creates an {@link AmqpConsumer} configured from the processor context and active AMQP
   * connection.
   *
   * <p>Evaluates {@code ADDRESS} expression language and reads {@code DESTINATION_TYPE}, {@code
   * AUTO_ACKNOWLEDGE}, {@code CREDIT_WINDOW}, and {@code RECEIVE_TIMEOUT} (defaulting to 15
   * seconds) before constructing the consumer. Synchronized to guard shared worker creation across
   * concurrent invocations.
   *
   * @param context the NiFi process context supplying processor property values
   * @param connection the active AMQP connection used to initialize the consumer
   * @return a configured {@link AmqpConsumer} instance
   * @throws ProcessException if initializing the consumer fails with a {@link ClientException}
   */
  @Override
  protected synchronized AmqpConsumer createAMQPWorker(
      ProcessContext context, Connection connection) {
    try {
      final String address = context.getProperty(ADDRESS).evaluateAttributeExpressions().getValue();
      final String destinationType = context.getProperty(DESTINATION_TYPE).getValue();
      final boolean autoAcknowledge = context.getProperty(AUTO_ACKNOWLEDGE).asBoolean();
      final int creditWindow = context.getProperty(CREDIT_WINDOW).asInteger();
      final var receiveTimeout =
          duration(context, RECEIVE_TIMEOUT, Duration.ofSeconds(15)).toMillis();
      return new AmqpConsumer(
          connection,
          address,
          destinationType.equalsIgnoreCase("TOPIC"),
          autoAcknowledge,
          creditWindow,
          receiveTimeout,
          getLogger());
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
