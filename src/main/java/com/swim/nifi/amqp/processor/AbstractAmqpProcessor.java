package com.swim.nifi.amqp.processor;

import com.swim.nifi.amqp.client.AmqpExceptionClassifier;
import com.swim.nifi.amqp.client.AmqpResource;
import com.swim.nifi.amqp.client.AmqpWorker;
import com.swim.nifi.amqp.exception.RetryableAmqpException;
import com.swim.nifi.amqp.exception.UnparseableAmqpMessageException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;

/**
 * Base processor that uses Apache Qpid ProtonJ2 to rendezvous with AMQP-based messaging systems
 * version 1.0.0
 */
abstract class AbstractAmqpProcessor<T extends AmqpWorker> extends AbstractProcessor {
  /** A property descriptor defining the target AMQP host used for connecting to the broker */
  public static final PropertyDescriptor HOST =
      new PropertyDescriptor.Builder()
          .name("Host")
          .description("AMQP broker host name or IP address.")
          .required(true)
          .defaultValue("localhost")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  /** A property descriptor defining the target AMQP port used for connecting to the broker */
  public static final PropertyDescriptor PORT =
      new PropertyDescriptor.Builder()
          .name("Port")
          .description("AMQP broker listener port.")
          .required(true)
          .defaultValue("5672")
          .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
          .build();

  /**
   * A property descriptor defining the target AMQP address (queue or topic name) used for
   * publishing or consuming messages.
   */
  public static final PropertyDescriptor ADDRESS =
      new PropertyDescriptor.Builder()
          .name("Address")
          .description(
              "AMQP receiver address. Broker-specific format (consult broker documentation). "
                  + "Most AMQP brokers accept 'queue://name' for queues. Topic subscription format varies by broker.")
          .required(true)
          .defaultValue("queue://example")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  /** A property descriptor defining the username used for connecting to the broker */
  public static final PropertyDescriptor USERNAME =
      new PropertyDescriptor.Builder()
          .name("Username")
          .description("Optional SASL user name for broker authentication.")
          .required(false)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  /** A property descriptor defining the password used for connecting to the broker */
  public static final PropertyDescriptor PASSWORD =
      new PropertyDescriptor.Builder()
          .name("Password")
          .description("Optional SASL password for broker authentication.")
          .required(false)
          .sensitive(true)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  /** A property descriptor defining the virtual host used for connecting to the broker */
  public static final PropertyDescriptor VIRTUAL_HOST =
      new PropertyDescriptor.Builder()
          .name("Virtual Host")
          .description(
              "Optional virtual host sent in the AMQP open frame. Leave blank to reuse the connect host.")
          .required(false)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  /** A property descriptor defining the reconnect enabled for connection option */
  public static final PropertyDescriptor RECONNECT_ENABLED =
      new PropertyDescriptor.Builder()
          .name("Reconnect Enabled")
          .description(
              "Whether the client should automatically reconnect after connection failure.")
          .required(true)
          .defaultValue("true")
          .allowableValues("true", "false")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build();

  /** A property descriptor defining the trace frames for connection option */
  public static final PropertyDescriptor TRACE_FRAMES =
      new PropertyDescriptor.Builder()
          .name("Trace Frames")
          .description(
              "Whether to enable low-level AMQP frame tracing for troubleshooting. Enable only for debugging.")
          .required(true)
          .defaultValue("false")
          .allowableValues("true", "false")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build();

  /**
   * A property descriptor indicating whether Transport Layer Security (TLS) should be enabled for
   * secure communication channels with the AMQP broker.
   */
  public static final PropertyDescriptor TLS_ENABLED =
      new PropertyDescriptor.Builder()
          .name("TLS Enabled")
          .description("Whether to use TLS/SSL for broker connection.")
          .required(true)
          .defaultValue("false")
          .allowableValues("true", "false")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build();

  /**
   * A property descriptor defining the NiFi SSL Context Service used to supply secure socket layer
   * (SSL/TLS) key store and trust store settings for broker connections.
   */
  public static final PropertyDescriptor SSL_CONTEXT_SERVICE =
      new PropertyDescriptor.Builder()
          .name("SSL Context Service")
          .description(
              "Standard NiFi SSLContextService used to supply broker key store and trust store settings.")
          .required(false)
          .identifiesControllerService(SSLContextService.class)
          .build();

  /**
   * A property descriptor that determines whether the client should verify the broker's hostname
   * against its TLS/SSL certificate during the secure handshake.
   */
  public static final PropertyDescriptor TLS_VERIFY_HOST =
      new PropertyDescriptor.Builder()
          .name("TLS Verify Host")
          .description("Whether to verify the broker's hostname in the certificate.")
          .required(true)
          .defaultValue("true")
          .allowableValues("true", "false")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build();

  /**
   * A property descriptor that determines whether to trust all server certificates during the TLS
   * handshake, typically used for testing or development environments to bypass strict certificate
   * verification.
   */
  public static final PropertyDescriptor TLS_TRUST_ALL =
      new PropertyDescriptor.Builder()
          .name("TLS Trust All")
          .description(
              "Whether to trust all server certificates. Use only for testing; disable verification with caution.")
          .required(true)
          .defaultValue("false")
          .allowableValues("true", "false")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build();

  /**
   * A property descriptor that determines whether a durable subscription should be established for
   * topic-based consumption, allowing the broker to retain and accumulate messages while the
   * consumer is disconnected or offline.
   */
  public static final PropertyDescriptor DURABLE_SUBSCRIPTION =
      new PropertyDescriptor.Builder()
          .name("Durable Subscription")
          .description(
              "Whether to create a durable subscription that survives client disconnections. "
                  + "True: Messages are buffered while consumer is offline. False: Messages only delivered when connected.")
          .required(true)
          .defaultValue("false")
          .allowableValues("true", "false")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build();

  private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS =
      List.of(
          HOST,
          PORT,
          ADDRESS,
          USERNAME,
          PASSWORD,
          VIRTUAL_HOST,
          RECONNECT_ENABLED,
          TRACE_FRAMES,
          TLS_ENABLED,
          SSL_CONTEXT_SERVICE,
          TLS_VERIFY_HOST,
          TLS_TRUST_ALL,
          DURABLE_SUBSCRIPTION);
  private BlockingQueue<AmqpResource<T>> resourceQueue;

  /**
   * Returns a list of common property descriptors shared across AMQP-related Apache NiFi
   * processors.
   *
   * @return a list of common {@link PropertyDescriptor} instances
   */
  protected static List<PropertyDescriptor> getCommonPropertyDescriptors() {
    return PROPERTY_DESCRIPTORS;
  }

  private static void classify(final Exception exception, final String message) {
    if (AmqpExceptionClassifier.isRetryable(exception)) {
      throw new RetryableAmqpException(message, exception);
    }
    throw new UnparseableAmqpMessageException(message, exception);
  }

  /**
   * Initializes and starts the processor when it is scheduled to run, setting up resources
   *
   * @param context the NiFi process context used to retrieve configured component properties
   */
  @OnScheduled
  public void onScheduled(ProcessContext context) {
    resourceQueue = new LinkedBlockingQueue<>(context.getMaxConcurrentTasks());
  }

  @Override
  public Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
    final List<ValidationResult> results = new ArrayList<>(1);
    final boolean usernameSet = validationContext.getProperty(USERNAME).isSet();
    final boolean passwordSet = validationContext.getProperty(PASSWORD).isSet();
    if ((usernameSet && !passwordSet) || (!usernameSet && passwordSet)) {
      results.add(
          new ValidationResult.Builder()
              .subject("Username and Password")
              .valid(false)
              .explanation("if username or password is set, both must be set.")
              .build());
    }
    return results;
  }

  /** Closes the underlying resources, shutting down any resource */
  @OnStopped
  public void close() {
    if (resourceQueue != null) {
      AmqpResource<T> resource;
      while ((resource = resourceQueue.poll()) != null) {
        closeResource(resource);
      }
      resourceQueue = null;
    }
  }

  private void closeResource(AmqpResource<T> resource) {
    try {
      resource.close();
    } catch (Exception e) {
      getLogger().error("Failed to close AMQP Connection", e);
    }
  }

  /**
   * Processes an active AMQP resource or worker action within the context of a NiFi session,
   * performing message consumption or publishing tasks using the established connection.
   *
   * @param connection the established AMQP connection
   * @param worker the underlying AMQP worker or resource instance (e.g., consumer or publisher)
   * @param context the NiFi process context for retrieving property values
   * @param session the NiFi process session for reading, writing, and routing FlowFiles
   * @throws ProcessException if an error occurs during processing
   */
  protected abstract void processResource(
      Connection connection, T worker, ProcessContext context, ProcessSession session)
      throws ProcessException;

  /**
   * Builds the appropriate AMQP Worker for the implementing processor
   *
   * @param context instance of {@link ProcessContext}
   * @param connection instance of {@link Connection}
   * @return new instance of {@link AmqpWorker}
   */
  protected abstract T createAMQPWorker(ProcessContext context, Connection connection);

  @Override
  public final void onTrigger(final ProcessContext context, final ProcessSession session)
      throws ProcessException {
    AmqpResource<T> resource = resourceQueue.poll();
    if (resource == null) {
      try {
        resource = createResource(context);
      } catch (Exception e) {
        getLogger().error("Failed to initialize AMQP client", e);
        context.yield();
        return;
      }
    }
    try {
      processResource(resource.connection(), resource.worker(), context, session);
      if (!resourceQueue.offer(resource)) {
        getLogger().info("Worker queue is full, closing AMQP client");
        closeResource(resource);
      }
    } catch (RetryableAmqpException | UnparseableAmqpMessageException e) {
      getLogger().error("AMQP failure, dropping the client", e);
      context.yield();
      closeResource(resource);
    } catch (Exception e) {
      getLogger().error("Processor failure, dropping the client", e);
      session.rollback();
      context.yield();
      closeResource(resource);
    }
  }

  private AmqpResource<T> createResource(final ProcessContext context) {
    Connection connection = null;
    try {
      connection = createConnection(context);
      T worker = createAMQPWorker(context, connection);
      return new AmqpResource<>(connection, worker);
    } catch (Exception e) {
      if (connection != null
          && !connection.openFuture().isDone()
          && !connection.openFuture().isCancelled()) {
        try {
          connection.close();
        } catch (Exception closingEx) {
          getLogger().error("Failed to close AMQP Connection", closingEx);
        }
      }
      throw e;
    }
  }

  /**
   * Creates a new AMQP connection using the provided ProcessContext.
   *
   * @param context instance of {@link ProcessContext}
   * @return new instance of {@link Connection}
   */
  protected Connection createConnection(ProcessContext context) {
    String host = context.getProperty(HOST).evaluateAttributeExpressions().getValue();
    int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
    getLogger().info("Opening AMQP connection to {}:{}", host, port);
    final Client client = Client.create();
    final ConnectionOptions options =
        new ConnectionOptions()
            .reconnectEnabled(
                context.getProperty(RECONNECT_ENABLED).evaluateAttributeExpressions().asBoolean())
            .traceFrames(
                context.getProperty(TRACE_FRAMES).evaluateAttributeExpressions().asBoolean())
            .idleTimeout(60000)
            .closeTimeout(10000);

    if (context.getProperty(USERNAME).isSet()) {
      options.user(context.getProperty(USERNAME).evaluateAttributeExpressions().getValue());
    }
    if (context.getProperty(PASSWORD).isSet()) {
      options.password(context.getProperty(PASSWORD).getValue());
    }
    if (context.getProperty(VIRTUAL_HOST).isSet()) {
      options.virtualHost(
          context.getProperty(VIRTUAL_HOST).evaluateAttributeExpressions().getValue());
    }
    options.sslOptions().verifyHost(context.getProperty(TLS_VERIFY_HOST).asBoolean());
    options.sslOptions().trustAll(context.getProperty(TLS_TRUST_ALL).asBoolean());
    final SSLContextService sslContextService =
        context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
    if (sslContextService != null) {
      options.sslOptions().sslEnabled(true);
      if (sslContextService.getKeyStoreFile() != null) {
        options.sslOptions().keyStoreLocation(sslContextService.getKeyStoreFile());
        options.sslOptions().keyStorePassword(sslContextService.getKeyStorePassword());
        options.sslOptions().keyStoreType(sslContextService.getKeyStoreType());
      }
      if (sslContextService.getTrustStoreFile() != null) {
        options.sslOptions().trustStoreLocation(sslContextService.getTrustStoreFile());
        options.sslOptions().trustStorePassword(sslContextService.getTrustStorePassword());
        options.sslOptions().trustStoreType(sslContextService.getTrustStoreType());
      }
    }

    try {
      final Connection connection = client.connect(host, port, options);
      getLogger()
          .info(
              "Successfully established AMQP connection to {}:{} (ID: {})",
              host,
              port,
              connection.hashCode());
      return connection;
    } catch (final RuntimeException exception) {
      getLogger()
          .error(
              "RuntimeException while connecting to {}:{} - {}",
              host,
              port,
              exception.getMessage(),
              exception);
      client.close();
      classify(exception, "Failed to open AMQP connection");
      throw exception;
    } catch (final Exception exception) {
      getLogger()
          .error(
              "Exception while connecting to {}:{} - {}",
              host,
              port,
              exception.getMessage(),
              exception);
      client.close();
      classify(exception, "Failed to open AMQP connection");
      throw new RetryableAmqpException("Failed to open AMQP connection", exception);
    }
  }
}
