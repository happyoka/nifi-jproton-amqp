package com.swim.nifi.amqp.client;

import com.swim.nifi.amqp.exception.RetryableAmqpException;
import com.swim.nifi.amqp.model.OutboundMessage;
import com.swim.nifi.amqp.support.PayloadGenerator;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.qpid.protonj2.client.*;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

/**
 * Generic publisher of messages from AMQP-based messaging system. It is based on JProton client API
 */
public class AmqpPublisher extends AmqpWorker {

  private final Sender sender;
  private final String address;
  private final long senderTimeout;
  private final PayloadGenerator payloadGenerator;

  /**
   * Constructs a new {@code AmqpPublisher} instance, initializes the payload generator, and opens
   * an anonymous sender link for publishing messages to the broker.
   *
   * @param connection the underlying AMQP client connection to the broker
   * @param address the target address associated with this publisher instance
   * @param senderTimeout the timeout duration in milliseconds for sending operations and
   *     acknowledgments
   * @param processorLog the logging component used to record publisher lifecycle events and errors
   * @throws ClientException if opening the anonymous sender link or initializing session resources
   *     fails
   */
  public AmqpPublisher(
      final Connection connection,
      final String address,
      final long senderTimeout,
      final ComponentLog processorLog)
      throws ClientException {
    super(connection, processorLog);
    this.payloadGenerator = new PayloadGenerator();
    this.senderTimeout = senderTimeout;
    this.address = address;
    this.sender = session.openAnonymousSender();
  }

  /**
   * Publishes a message to the AMQP broker using the provided FlowFile and ProcessSession. The
   * message is constructed based on the FlowFile attributes and payload, and sent to the broker
   * using the configured sender. If the message fails to be sent or acknowledged, a
   * RetryableAmqpException is thrown to indicate that the operation can be retried.
   *
   * @param flowFile instance of {@link FlowFile}
   * @param session instance of {@link ProcessSession}
   */
  public void publish(final FlowFile flowFile, final ProcessSession session) {
    final OutboundMessage outboundMessage =
        mapper.toOutboundMessage(
            flowFile.getAttributes(), payloadGenerator.read(session, flowFile), null);
    final String address =
        outboundMessage.address() != null && !outboundMessage.address().isBlank()
            ? outboundMessage.address()
            : this.address;
    try {
      final Message<byte[]> message =
          mapper.toMessage(
              new OutboundMessage(
                  outboundMessage.body(),
                  address,
                  outboundMessage.messageId(),
                  outboundMessage.creationTime(),
                  outboundMessage.correlationId(),
                  outboundMessage.subject(),
                  outboundMessage.replyTo(),
                  outboundMessage.contentType(),
                  outboundMessage.contentEncoding(),
                  outboundMessage.applicationProperties(),
                  outboundMessage.deliveryAnnotations(),
                  outboundMessage.durable(),
                  outboundMessage.priority(),
                  outboundMessage.timeToLive()));
      final Tracker tracker =
          outboundMessage.deliveryAnnotations().isEmpty()
              ? sender.send(message)
              : sender.send(message, outboundMessage.deliveryAnnotations());
      tracker.awaitAccepted(senderTimeout, TimeUnit.MILLISECONDS);
    } catch (final ClientException ex) {
      this.processorLog.error(
          "Failed to publish message because the AMQP connection is lost or has been closed", ex);
      throw new RetryableAmqpException("Failed to publish message", ex);
    } catch (final Exception ex) {
      this.processorLog.error("Failed to publish message", ex);
      throw new RetryableAmqpException("Failed to publish message", ex);
    }
  }

  @Override
  public void onProcessorStopped() {
    this.sender.close();
  }
}
