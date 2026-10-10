package com.swim.nifi.amqp.client;

import com.swim.nifi.amqp.exception.UnparseableAmqpMessageException;
import com.swim.nifi.amqp.model.ConsumedMessage;
import com.swim.nifi.amqp.model.InboundMessage;
import com.swim.nifi.amqp.model.SettlementMode;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.qpid.protonj2.client.*;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

/**
 * Generic consumer of messages from AMQP-based messaging system. It is based on JProton client API
 */
public class AmqpConsumer extends AmqpWorker implements Runnable {

  private final AtomicBoolean running = new AtomicBoolean(true);
  private final AtomicBoolean shutDownTrigger = new AtomicBoolean(false);
  private final String address;
  private final Receiver receiver;
  private final long receiveTimeout;
  private final int creditWindow;
  private final BlockingQueue<ConsumedMessage> buffer = new LinkedBlockingQueue<>(1024);
  private final AtomicBoolean backpressureLogged = new AtomicBoolean(false);
  private final ExecutorService executorService;

  /**
   * Constructs a new {@code AmqpConsumer} instance, configures receiver options, opens a receiver
   * link for the specified address, and spins up a dedicated single-thread background executor to
   * begin consuming messages asynchronously.
   *
   * @param connection the underlying AMQP client connection to the broker
   * @param address the target address (queue or topic) to consume messages from
   * @param autoAcknowledge whether messages should be automatically accepted upon delivery
   * @param creditWindow the flow control credit window size; a value of {@code 0} requires manual
   *     credit replenishment via {@code receiver.addCredit(1)}
   * @param receiveTimeout the timeout duration in milliseconds for polling deliveries (clamped to a
   *     minimum of 250ms)
   * @param processorLog the logging component used to record consumer lifecycle events and errors
   * @throws ClientException if opening the receiver link, configuring options, or starting the
   *     background execution thread fails
   */
  public AmqpConsumer(
      final Connection connection,
      final String address,
      final boolean autoAcknowledge,
      final int creditWindow,
      final long receiveTimeout,
      final ComponentLog processorLog)
      throws ClientException {
    super(connection, processorLog);
    this.receiveTimeout = Math.max(250, receiveTimeout);
    this.address = address;
    this.creditWindow = creditWindow;
    final var receiverOptions =
        new ReceiverOptions().autoAccept(autoAcknowledge).creditWindow(creditWindow);
    this.receiver = session.openReceiver(this.address, receiverOptions);
    if (this.creditWindow == 0) {
      receiver.addCredit(1);
    }
    this.processorLog.info(
        "Successfully connected AMQPConsumer to {} and '{}' address", connection, address);
    this.executorService =
        Executors.newSingleThreadExecutor(
            BasicThreadFactory.builder().namingPattern("AMQP Client: ").build());
    this.executorService.submit(this);
  }

  private static Map<String, Object> copyProperties(final Message<?> message) {
    final Map<String, Object> properties = new LinkedHashMap<>();
    try {
      message.forEachProperty(properties::put);
    } catch (final ClientException exception) {
      throw new UnparseableAmqpMessageException(
          "Failed to read AMQP message properties", exception);
    }
    return properties;
  }

  private ConsumedMessage toConsumedMessage(final Delivery delivery) {
    try {
      final Message<?> message = delivery.message();
      final Map<String, Object> annotations =
          Optional.ofNullable(delivery.annotations()).orElseGet(LinkedHashMap::new);
      final InboundMessage inboundMessage =
          new InboundMessage(
              mapper.toPayloadBytes(message.body()),
              stringValue(message.messageId()),
              stringValue(message.correlationId()),
              message.subject(),
              message.to(),
              message.replyTo(),
              message.contentType(),
              message.contentEncoding(),
              message.durable(),
              message.priority(),
              message.timeToLive(),
              message.creationTime(),
              copyProperties(message),
              annotations);

      final Map<String, String> attributes = mapper.toFlowFileAttributes(inboundMessage);
      final DeliveryAcknowledgement acknowledgement = new DeliveryAcknowledgement(delivery);
      return new ConsumedMessage(
          inboundMessage.body(),
          attributes,
          acknowledgement,
          SettlementMode.ACCEPTED,
          SettlementMode.RELEASED);
    } catch (final Exception exception) {
      throw new UnparseableAmqpMessageException(
          "Failed to decode consumed AMQP message", exception);
    }
  }

  private void rejectMalformedDelivery(
      final Delivery delivery, final UnparseableAmqpMessageException exception) {
    try {
      delivery.reject("amqp:malformed-message", exception.getMessage());
    } catch (final Exception settlementException) {
      processorLog.warn(
          "Failed to reject malformed AMQP delivery after decode failure: {}",
          settlementException.getMessage(),
          settlementException);
    }
  }

  private void releaseBufferedMessage(final ConsumedMessage consumedMessage) {
    try {
      consumedMessage.acknowledgement().settle(consumedMessage.retrySettlement());
    } catch (final Exception exception) {
      processorLog.warn(
          "Failed to release buffered AMQP delivery during subscription shutdown: {}",
          exception.getMessage(),
          exception);
    }
  }

  private boolean enqueueOrRelease(final ConsumedMessage consumedMessage)
      throws InterruptedException {
    while (running.get()) {
      if (buffer.offer(consumedMessage, this.receiveTimeout, TimeUnit.MILLISECONDS)) {
        backpressureLogged.set(false);
        return true;
      }
      if (backpressureLogged.compareAndSet(false, true)) {
        processorLog.warn(
            "Active AMQP subscription buffer is full at {}; waiting for NiFi to drain messages",
            address);
      }
    }
    releaseBufferedMessage(consumedMessage);
    return false;
  }

  @Override
  public void run() {
    processorLog.info("Active AMQP subscription listener started for at {}", this.address);
    try {
      while (running.get() && !Thread.currentThread().isInterrupted()) {
        final Delivery delivery = receiver.receive(receiveTimeout, TimeUnit.MILLISECONDS);
        if (delivery == null) {
          continue;
        }
        final ConsumedMessage consumedMessage;
        try {
          consumedMessage = toConsumedMessage(delivery);
        } catch (final UnparseableAmqpMessageException exception) {
          rejectMalformedDelivery(delivery, exception);
          continue;
        }
        if (!enqueueOrRelease(consumedMessage)) {
          break;
        }
        if (this.creditWindow == 0) {
          receiver.addCredit(1);
        }
      }
    } catch (final InterruptedException exception) {
      Thread.currentThread().interrupt();
    } catch (final Exception exception) {
      if (!shutDownTrigger.get()) {
        processorLog.error(
            "Unexpected error in AMQP subscription loop at {} stopped due to failure: {}",
            address,
            exception.getMessage());
      }
    } finally {
      running.set(false);
      if (!shutDownTrigger.get()) {
        this.close();
      }
      processorLog.info("Active AMQP subscription listener stopped at {}", address);
    }
  }

  /**
   * Consumes message from the target identified by 'address'. In the event there are no messages in
   * the queue it will return Optional.empty().
   *
   * @return instance of {@link ConsumedMessage}
   */
  public Optional<ConsumedMessage> poll() {
    final ConsumedMessage message = buffer.poll();
    if (message != null) {
      return Optional.of(message);
    }
    return Optional.empty();
  }

  /**
   * Creates a FlowFile containing the AMQP message body and mapped metadata.
   *
   * @param session NiFi process session used to create and write the FlowFile
   * @param consumedMessage received AMQP message
   * @return FlowFile ready for transfer
   */
  public FlowFile toFlowFile(final ProcessSession session, final ConsumedMessage consumedMessage) {
    var flowFile = session.create();
    flowFile = session.write(flowFile, outputStream -> outputStream.write(consumedMessage.body()));
    flowFile = session.putAllAttributes(flowFile, consumedMessage.attributes());
    return flowFile;
  }

  @Override
  public void onProcessorStopped() {
    shutDownTrigger.set(true);
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(2, TimeUnit.SECONDS)) {
        executorService.shutdownNow(); // Cancel currently executing tasks
        if (!executorService.awaitTermination(3, TimeUnit.SECONDS)) {
          processorLog.error("ExecutorService did not terminate");
        }
      }
    } catch (InterruptedException ie) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
    try {
      this.receiver.close();
    } finally {
      try {
        // simply discard the messages, all unacknowledged messages will be redelivered by the
        // broker when the consumer connects again
        buffer.clear();
      } catch (Exception e) {
        processorLog.error("Failed to drain response queue.");
      }
    }
  }
}
