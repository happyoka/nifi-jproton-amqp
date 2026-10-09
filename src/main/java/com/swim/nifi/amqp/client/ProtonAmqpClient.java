package com.swim.nifi.amqp.client;

import com.swim.nifi.amqp.config.AmqpExtensionProperties;
import com.swim.nifi.amqp.exception.RetryableAmqpException;
import com.swim.nifi.amqp.exception.UnparseableAmqpMessageException;
import com.swim.nifi.amqp.mapping.FlowFileAttributeMapper;
import com.swim.nifi.amqp.model.ConsumedMessage;
import com.swim.nifi.amqp.model.InboundMessage;
import com.swim.nifi.amqp.model.OutboundMessage;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ProtonJ2-based AMQP client used by the NiFi processors. */
public final class ProtonAmqpClient implements AmqpClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProtonAmqpClient.class);
  private static final int SUBSCRIPTION_BUFFER_CAPACITY = 1024;

  private final AmqpExtensionProperties properties;
  private final FlowFileAttributeMapper mapper =
      new FlowFileAttributeMapper(new com.fasterxml.jackson.databind.ObjectMapper());
  private final ConcurrentMap<String, ManagedConnection> connections = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ManagedSubscription> activeSubscriptions =
      new ConcurrentHashMap<>();

  /**
   * Creates a new transport facade backed by shared ProtonJ2 connections keyed by profile name.
   *
   * @param properties resolved extension properties
   */
  public ProtonAmqpClient(final AmqpExtensionProperties properties) {
    this.properties = properties;
  }

  private static long timeout(final Duration fallback) {
    return fallback.toMillis();
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

  private static String stringValue(final Object value) {
    return value == null ? null : String.valueOf(value);
  }

  private static void classify(final Exception exception, final String message) {
    if (AmqpExceptionClassifier.isRetryable(exception)) {
      throw new RetryableAmqpException(message, exception);
    }
    throw new UnparseableAmqpMessageException(message, exception);
  }

  private long subscriptionWaitTimeoutMillis() {
    return Math.max(250L, timeout(properties.getDefaults().getReceiveTimeout()));
  }

  @Override
  public void publish(final String profileName, final OutboundMessage outboundMessage) {
    final AmqpExtensionProperties.Broker broker = properties.getRequiredBroker(profileName);
    final ManagedConnection managedConnection = getOrCreateConnection(profileName, broker);
    final String address =
        outboundMessage.address() != null && !outboundMessage.address().isBlank()
            ? outboundMessage.address()
            : broker.getAddress();
    try {
      try (Session session = managedConnection.connection().openSession();
          Sender sender = session.openSender(address)) {
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
        tracker.awaitAccepted(
            timeout(properties.getDefaults().getSendTimeout()), TimeUnit.MILLISECONDS);
      }
    } catch (final Exception exception) {
      invalidateConnectionIfRetryable(profileName, managedConnection, exception);
      classify(exception, "Failed to publish AMQP message to " + address);
    }
  }

  @Override
  public void subscribe(final String profileName, final String addressOverride) {
    final AmqpExtensionProperties.Broker broker = properties.getRequiredBroker(profileName);
    LOGGER.info(
        "Subscribing to long-lived consumer - Profile: {}, Broker: {}:{}, Address: {}",
        profileName,
        broker.getHost(),
        broker.getPort(),
        addressOverride != null && !addressOverride.isBlank()
            ? addressOverride
            : broker.getAddress());

    final ManagedConnection managedConnection = getOrCreateConnection(profileName, broker);
    final String address =
        addressOverride != null && !addressOverride.isBlank()
            ? addressOverride
            : broker.getAddress();

    try {
      activeSubscriptions.compute(
          profileName,
          (key, existing) -> {
            if (existing != null && existing.isRunning()) {
              return existing;
            }

            if (existing != null) {
              existing.stop();
            }

            final ManagedSubscription managedSubscription =
                createManagedSubscription(profileName, address, managedConnection, broker);
            managedSubscription.start();
            LOGGER.info(
                "Long-lived subscription established for {} on profile {}", address, profileName);
            return managedSubscription;
          });
    } catch (final Exception exception) {
      invalidateConnectionIfRetryable(profileName, managedConnection, exception);
      classify(exception, "Failed to subscribe to AMQP message source " + address);
    }
  }

  @Override
  public Optional<ConsumedMessage> poll(final String profileName) {
    final ManagedSubscription subscription = activeSubscriptions.get(profileName);
    if (subscription == null) {
      throw new RetryableAmqpException(
          "No active subscription for profile " + profileName + "; call subscribe() first");
    }

    final RuntimeException pendingFailure = subscription.pollPendingFailure();
    if (pendingFailure instanceof UnparseableAmqpMessageException unparseable) {
      throw unparseable;
    }
    if (pendingFailure != null) {
      throw pendingFailure;
    }

    final ConsumedMessage message = subscription.poll();
    if (message != null) {
      return Optional.of(message);
    }

    if (!subscription.isRunning()) {
      activeSubscriptions.remove(profileName, subscription);
      subscription.closeResources();
      final Throwable failure = subscription.failure();
      if (failure instanceof UnparseableAmqpMessageException unparseable) {
        throw unparseable;
      }
      if (failure instanceof RetryableAmqpException retryable) {
        throw retryable;
      }
      if (failure != null) {
        throw new RetryableAmqpException("Failed to poll AMQP subscription", failure);
      }
      throw new RetryableAmqpException(
          "Active AMQP subscription is closed for profile " + profileName);
    }

    return Optional.empty();
  }

  @Override
  public void unsubscribe(final String profileName) {
    final ManagedSubscription subscription = activeSubscriptions.remove(profileName);
    if (subscription != null) {
      subscription.stop();
      LOGGER.info("Long-lived subscription closed for profile {}", profileName);
    }
  }

  @Override
  public void close() {
    activeSubscriptions.forEach((profileName, subscription) -> subscription.stop());
    activeSubscriptions.clear();
    connections.values().forEach(ManagedConnection::close);
    connections.clear();
  }

  private ManagedConnection openConnection(final AmqpExtensionProperties.Broker broker) {
    LOGGER.info("Opening AMQP connection to {}:{}", broker.getHost(), broker.getPort());
    final Client client = Client.create();
    final ConnectionOptions options =
        new ConnectionOptions()
            .reconnectEnabled(properties.getDefaults().isReconnectEnabled())
            .traceFrames(properties.getDefaults().isTraceFrames())
            .idleTimeout(60000)
            .closeTimeout(10000);

    if (broker.getUsername() != null) {
      options.user(broker.getUsername());
    }
    if (broker.getPassword() != null) {
      options.password(broker.getPassword());
    }
    if (broker.getVirtualHost() != null) {
      options.virtualHost(broker.getVirtualHost());
    }

    options.saslOptions().saslEnabled(broker.getSasl().isEnabled());
    broker.getSasl().getMechanisms().forEach(options.saslOptions()::addAllowedMechanism);

    options.sslEnabled(broker.getTls().isEnabled());
    options.sslOptions().sslEnabled(broker.getTls().isEnabled());
    options.sslOptions().verifyHost(broker.getTls().isVerifyHost());
    options.sslOptions().trustAll(broker.getTls().isTrustAll());
    options
        .sslOptions()
        .enabledProtocols(broker.getTls().getEnabledProtocols().toArray(String[]::new));
    if (broker.getTls().getKeyStoreLocation() != null) {
      options.sslOptions().keyStoreLocation(broker.getTls().getKeyStoreLocation());
      options.sslOptions().keyStorePassword(broker.getTls().getKeyStorePassword());
      options.sslOptions().keyStoreType(broker.getTls().getKeyStoreType());
      if (broker.getTls().getKeyAlias() != null) {
        options.sslOptions().keyAlias(broker.getTls().getKeyAlias());
      }
    }
    if (broker.getTls().getTrustStoreLocation() != null) {
      options.sslOptions().trustStoreLocation(broker.getTls().getTrustStoreLocation());
      options.sslOptions().trustStorePassword(broker.getTls().getTrustStorePassword());
      options.sslOptions().trustStoreType(broker.getTls().getTrustStoreType());
    }

    try {
      final Connection connection = client.connect(broker.getHost(), broker.getPort(), options);
      LOGGER.info(
          "Successfully established AMQP connection to {}:{} (ID: {})",
          broker.getHost(),
          broker.getPort(),
          connection.hashCode());
      return new ManagedConnection(client, connection);
    } catch (final RuntimeException exception) {
      LOGGER.error(
          "RuntimeException while connecting to {}:{} - {}",
          broker.getHost(),
          broker.getPort(),
          exception.getMessage(),
          exception);
      client.close();
      classify(exception, "Failed to open AMQP connection");
      throw exception;
    } catch (final Exception exception) {
      LOGGER.error(
          "Exception while connecting to {}:{} - {}",
          broker.getHost(),
          broker.getPort(),
          exception.getMessage(),
          exception);
      client.close();
      classify(exception, "Failed to open AMQP connection");
      throw new RetryableAmqpException("Failed to open AMQP connection", exception);
    }
  }

  private ManagedConnection getOrCreateConnection(
      final String profileName, final AmqpExtensionProperties.Broker broker) {
    return connections.compute(
        profileName,
        (key, existing) ->
            existing == null || existing.isClosed() ? openConnection(broker) : existing);
  }

  private void invalidateConnectionIfRetryable(
      final String profileName,
      final ManagedConnection managedConnection,
      final Exception exception) {
    if (AmqpExceptionClassifier.isRetryable(exception)) {
      connections.computeIfPresent(
          profileName,
          (key, existing) -> {
            if (existing == managedConnection) {
              existing.close();
              return null;
            }
            return existing;
          });
    }
  }

  private ConsumedMessage toConsumedMessage(
      final AmqpExtensionProperties.Broker broker, final Delivery delivery) {
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
          broker.getSettlement().getSuccess(),
          broker.getSettlement().getRetry());
    } catch (final Exception exception) {
      throw new UnparseableAmqpMessageException(
          "Failed to decode consumed AMQP message", exception);
    }
  }

  private ManagedSubscription createManagedSubscription(
      final String profileName,
      final String address,
      final ManagedConnection managedConnection,
      final AmqpExtensionProperties.Broker broker) {
    try {
      final ReceiverOptions receiverOptions =
          new ReceiverOptions()
              .autoAccept(false)
              .creditWindow(properties.getDefaults().getCreditWindow());

      final Session session = managedConnection.connection().openSession();
      final Receiver receiver = session.openReceiver(address, receiverOptions);
      if (properties.getDefaults().getCreditWindow() == 0) {
        receiver.addCredit(1);
      }

      return new ManagedSubscription(
          profileName, address, managedConnection, broker, receiver, session);
    } catch (final Exception exception) {
      invalidateConnectionIfRetryable(profileName, managedConnection, exception);
      if (AmqpExceptionClassifier.isRetryable(exception)) {
        throw new RetryableAmqpException(
            "Failed to subscribe to AMQP message source " + address, exception);
      }
      throw new UnparseableAmqpMessageException(
          "Failed to subscribe to AMQP message source " + address, exception);
    }
  }

  private void rejectMalformedDelivery(
      final Delivery delivery, final UnparseableAmqpMessageException exception) {
    try {
      delivery.reject("amqp:malformed-message", exception.getMessage());
    } catch (final Exception settlementException) {
      LOGGER.warn(
          "Failed to reject malformed AMQP delivery after decode failure: {}",
          settlementException.getMessage(),
          settlementException);
    }
  }

  private void releaseBufferedMessage(final ConsumedMessage consumedMessage) {
    try {
      consumedMessage.acknowledgement().settle(consumedMessage.retrySettlement());
    } catch (final Exception exception) {
      LOGGER.warn(
          "Failed to release buffered AMQP delivery during subscription shutdown: {}",
          exception.getMessage(),
          exception);
    }
  }

  private static final class ManagedConnection {

    private final Client client;
    private final Connection connection;
    private volatile boolean closed;

    private ManagedConnection(final Client client, final Connection connection) {
      this.client = client;
      this.connection = connection;
    }

    private Connection connection() {
      return connection;
    }

    private boolean isClosed() {
      return closed;
    }

    private void close() {
      if (closed) {
        return;
      }
      closed = true;
      try {
        connection.close();
      } catch (final RuntimeException exception) {
        LOGGER.warn("Failed to close AMQP connection cleanly", exception);
      } finally {
        try {
          client.close();
        } catch (final RuntimeException exception) {
          LOGGER.warn("Failed to close AMQP client cleanly", exception);
        }
      }
    }
  }

  private final class ManagedSubscription implements Runnable {

    private final String profileName;
    private final String address;
    private final ManagedConnection managedConnection;
    private final AmqpExtensionProperties.Broker broker;
    private final Receiver receiver;
    private final Session session;
    private final BlockingQueue<ConsumedMessage> buffer =
        new LinkedBlockingQueue<>(SUBSCRIPTION_BUFFER_CAPACITY);
    private final AtomicReference<RuntimeException> pendingFailure = new AtomicReference<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean backpressureLogged = new AtomicBoolean(false);
    private final AtomicBoolean resourcesClosed = new AtomicBoolean(false);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final Thread listenerThread;

    private ManagedSubscription(
        final String profileName,
        final String address,
        final ManagedConnection managedConnection,
        final AmqpExtensionProperties.Broker broker,
        final Receiver receiver,
        final Session session) {
      this.profileName = profileName;
      this.address = address;
      this.managedConnection = managedConnection;
      this.broker = broker;
      this.receiver = receiver;
      this.session = session;
      this.listenerThread = new Thread(this, "amqp-subscription-" + profileName);
      this.listenerThread.setDaemon(true);
    }

    private void start() {
      listenerThread.start();
    }

    private boolean isRunning() {
      return running.get();
    }

    private Throwable failure() {
      return failure.get();
    }

    private RuntimeException pollPendingFailure() {
      return pendingFailure.getAndSet(null);
    }

    private ConsumedMessage poll() {
      return buffer.poll();
    }

    private void stop() {
      running.set(false);
      listenerThread.interrupt();
      closeResources();
    }

    private void recordPendingFailure(final RuntimeException exception) {
      pendingFailure.compareAndSet(null, exception);
    }

    private boolean enqueueOrRelease(final ConsumedMessage consumedMessage)
        throws InterruptedException {
      while (running.get()) {
        if (buffer.offer(consumedMessage, subscriptionWaitTimeoutMillis(), TimeUnit.MILLISECONDS)) {
          backpressureLogged.set(false);
          return true;
        }

        if (backpressureLogged.compareAndSet(false, true)) {
          LOGGER.warn(
              "Active AMQP subscription buffer is full for profile {} at {}; waiting for NiFi to drain messages",
              profileName,
              address);
        }
      }

      releaseBufferedMessage(consumedMessage);
      return false;
    }

    private void closeResources() {
      if (!resourcesClosed.compareAndSet(false, true)) {
        return;
      }

      try {
        receiver.close();
      } catch (final RuntimeException exception) {
        LOGGER.warn("Failed to close AMQP receiver cleanly", exception);
      }

      try {
        session.close();
      } catch (final RuntimeException exception) {
        LOGGER.warn("Failed to close AMQP session cleanly", exception);
      }
    }

    @Override
    public void run() {
      LOGGER.info(
          "Active AMQP subscription listener started for profile {} at {}", profileName, address);
      try {
        while (running.get()) {
          final Delivery delivery =
              receiver.receive(subscriptionWaitTimeoutMillis(), TimeUnit.MILLISECONDS);
          if (delivery == null) {
            continue;
          }

          final ConsumedMessage consumedMessage;
          try {
            consumedMessage = toConsumedMessage(broker, delivery);
          } catch (final UnparseableAmqpMessageException exception) {
            rejectMalformedDelivery(delivery, exception);
            recordPendingFailure(exception);
            continue;
          }

          if (!enqueueOrRelease(consumedMessage)) {
            break;
          }

          if (properties.getDefaults().getCreditWindow() == 0) {
            receiver.addCredit(1);
          }
        }
      } catch (final InterruptedException exception) {
        Thread.currentThread().interrupt();
        if (running.get()) {
          failure.compareAndSet(null, exception);
        }
      } catch (final Exception exception) {
        if (running.get()) {
          failure.compareAndSet(null, exception);
          invalidateConnectionIfRetryable(profileName, managedConnection, exception);
          LOGGER.warn(
              "Active AMQP subscription for profile {} at {} stopped due to failure: {}",
              profileName,
              address,
              exception.getMessage(),
              exception);
        }
      } finally {
        running.set(false);
        closeResources();
        LOGGER.info(
            "Active AMQP subscription listener stopped for profile {} at {}", profileName, address);
      }
    }
  }
}
