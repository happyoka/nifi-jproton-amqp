package com.swim.nifi.amqp.processor;

import com.swim.nifi.amqp.config.AmqpExtensionProperties;
import com.swim.nifi.amqp.model.SettlementMode;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

/** Utility methods and descriptors for property-driven AMQP processor configuration. */
final class AmqpProcessorPropertySupport {
  static final String DEFAULT_PROFILE = "default";
  static final PropertyDescriptor HOST =
      descriptor("Host", "AMQP broker host name or IP address.")
          .required(true)
          .defaultValue("localhost")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();
  static final PropertyDescriptor PORT =
      descriptor("Port", "AMQP broker listener port.")
          .required(true)
          .defaultValue("5672")
          .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
          .build();
  static final PropertyDescriptor ADDRESS =
      descriptor(
              "Address",
              "AMQP receiver address. Broker-specific format (consult broker documentation). "
                  + "Most AMQP brokers accept 'queue://name' for queues. Topic subscription format varies by broker.")
          .required(true)
          .defaultValue("queue://example")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();
  static final PropertyDescriptor USERNAME =
      descriptor("Username", "Optional SASL user name for broker authentication.")
          .required(false)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();
  static final PropertyDescriptor PASSWORD =
      descriptor("Password", "Optional SASL password for broker authentication.")
          .required(false)
          .sensitive(true)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();
  static final PropertyDescriptor VIRTUAL_HOST =
      descriptor(
              "Virtual Host",
              "Optional virtual host sent in the AMQP open frame. Leave blank to reuse the connect host.")
          .required(false)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();
  static final PropertyDescriptor MAX_CONCURRENT_OPERATIONS =
      descriptor(
              "Max Concurrent Operations",
              "Upper bound for concurrent broker operations performed by this processor instance.")
          .required(true)
          .defaultValue("64")
          .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
          .build();
  static final PropertyDescriptor SEND_TIMEOUT =
      descriptor("Send Timeout", "Maximum wait for remote settlement when publishing.")
          .required(true)
          .defaultValue("15 sec")
          .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
          .build();
  static final PropertyDescriptor RECEIVE_TIMEOUT =
      descriptor("Receive Timeout", "Maximum wait for a message before yielding.")
          .required(true)
          .defaultValue("5 sec")
          .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
          .build();
  static final PropertyDescriptor COMMIT_TIMEOUT =
      descriptor(
              "Commit Timeout",
              "Repository commit callback budget reserved for consume acknowledgements.")
          .required(true)
          .defaultValue("30 sec")
          .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
          .build();
  static final PropertyDescriptor CREDIT_WINDOW =
      descriptor(
              "Credit Window", "Receiver credit window. Set to 0 to request one message at a time.")
          .required(true)
          .defaultValue("25")
          .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
          .build();
  static final PropertyDescriptor RECONNECT_ENABLED =
      descriptor(
              "Reconnect Enabled",
              "Whether the client should automatically reconnect after connection failure.")
          .required(true)
          .defaultValue("true")
          .allowableValues("true", "false")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build();
  static final PropertyDescriptor TRACE_FRAMES =
      descriptor(
              "Trace Frames",
              "Whether to enable low-level AMQP frame tracing for troubleshooting. Enable only for debugging.")
          .required(true)
          .defaultValue("false")
          .allowableValues("true", "false")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build();
  static final PropertyDescriptor TLS_ENABLED =
      descriptor("TLS Enabled", "Whether to use TLS/SSL for broker connection.")
          .required(true)
          .defaultValue("false")
          .allowableValues("true", "false")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build();
  static final PropertyDescriptor SSL_CONTEXT_SERVICE =
      descriptor(
              "SSL Context Service",
              "Standard NiFi SSLContextService used to supply broker key store and trust store settings.")
          .required(false)
          .identifiesControllerService(SSLContextService.class)
          .build();
  static final PropertyDescriptor TLS_VERIFY_HOST =
      descriptor("TLS Verify Host", "Whether to verify the broker's hostname in the certificate.")
          .required(true)
          .defaultValue("true")
          .allowableValues("true", "false")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build();
  static final PropertyDescriptor TLS_TRUST_ALL =
      descriptor(
              "TLS Trust All",
              "Whether to trust all server certificates. Use only for testing; disable verification with caution.")
          .required(true)
          .defaultValue("false")
          .allowableValues("true", "false")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build();
  static final PropertyDescriptor SUCCESS_SETTLEMENT =
      settlementDescriptor(
          "Success Settlement",
          "Settlement applied after a successful FlowFile commit.",
          "ACCEPTED");
  static final PropertyDescriptor FAILURE_SETTLEMENT =
      settlementDescriptor(
          "Failure Settlement",
          "Settlement applied after a non-retryable parsing or mapping failure.",
          "REJECTED");
  static final PropertyDescriptor RETRY_SETTLEMENT =
      settlementDescriptor(
          "Retry Settlement",
          "Settlement applied after a retryable broker or repository failure.",
          "RELEASED");
  static final PropertyDescriptor DURABLE_SUBSCRIPTION =
      descriptor(
              "Durable Subscription",
              "Whether to create a durable subscription that survives client disconnections. "
                  + "True: Messages are buffered while consumer is offline. False: Messages only delivered when connected.")
          .required(true)
          .defaultValue("false")
          .allowableValues("true", "false")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build();
  static final PropertyDescriptor LINK_DURABILITY =
      descriptor(
              "Link Durability",
              "AMQP link durability mode. NONE: Non-durable (default). CONFIGURATION: Durable by broker configuration. "
                  + "UNSETTLED: Unsettled delivery mode for guaranteed delivery tracking.")
          .required(true)
          .defaultValue("NONE")
          .allowableValues("NONE", "CONFIGURATION", "UNSETTLED")
          .build();
  static final PropertyDescriptor AUTO_ACKNOWLEDGE =
      descriptor(
              "Auto-Acknowledge Messages",
              "Whether to automatically acknowledge received messages. "
                  + "True: Messages auto-acknowledged immediately (faster but less safe). "
                  + "False: Manual acknowledgement via settlement (default, safer).")
          .required(true)
          .defaultValue("false")
          .allowableValues("true", "false")
          .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
          .build();
  static final PropertyDescriptor EXPIRY_POLICY =
      descriptor(
              "Message Expiry Policy",
              "How to handle expired messages. LINK-DETACH: Detach link on expiry. "
                  + "SESSION-END: End session on expiry. CONNECTION-DROP: Drop connection on expiry.")
          .required(true)
          .defaultValue("LINK-DETACH")
          .allowableValues("LINK-DETACH", "SESSION-END", "CONNECTION-DROP")
          .build();
  private static final List<String> INTERNAL_TLS_PROTOCOLS = List.of("TLSv1.3", "TLSv1.2");
  private static final List<String> INTERNAL_SASL_MECHANISMS = List.of("PLAIN");
  private static final List<PropertyDescriptor> DESCRIPTORS =
      List.of(
          HOST,
          PORT,
          ADDRESS,
          USERNAME,
          PASSWORD,
          VIRTUAL_HOST,
          MAX_CONCURRENT_OPERATIONS,
          SEND_TIMEOUT,
          RECEIVE_TIMEOUT,
          COMMIT_TIMEOUT,
          CREDIT_WINDOW,
          RECONNECT_ENABLED,
          TRACE_FRAMES,
          TLS_ENABLED,
          SSL_CONTEXT_SERVICE,
          TLS_VERIFY_HOST,
          TLS_TRUST_ALL,
          SUCCESS_SETTLEMENT,
          FAILURE_SETTLEMENT,
          RETRY_SETTLEMENT,
          DURABLE_SUBSCRIPTION,
          LINK_DURABILITY,
          AUTO_ACKNOWLEDGE,
          EXPIRY_POLICY);

  private AmqpProcessorPropertySupport() {}

  static List<PropertyDescriptor> descriptors() {
    return DESCRIPTORS;
  }

  static AmqpExtensionProperties toProperties(final ProcessContext context) {
    final AmqpExtensionProperties.Defaults defaults = new AmqpExtensionProperties.Defaults();
    defaults.setMaxConcurrentOperations(integer(context, MAX_CONCURRENT_OPERATIONS, 64));
    defaults.setSendTimeout(duration(context, SEND_TIMEOUT, Duration.ofSeconds(15)));
    defaults.setReceiveTimeout(duration(context, RECEIVE_TIMEOUT, Duration.ofSeconds(5)));
    defaults.setCommitTimeout(duration(context, COMMIT_TIMEOUT, Duration.ofSeconds(30)));
    defaults.setCreditWindow(integer(context, CREDIT_WINDOW, 25));
    defaults.setReconnectEnabled(bool(context, RECONNECT_ENABLED, true));
    defaults.setTraceFrames(bool(context, TRACE_FRAMES, false));

    final SSLContextService sslContextService =
        context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

    final AmqpExtensionProperties.Tls tls = new AmqpExtensionProperties.Tls();
    tls.setEnabled(sslContextService != null || bool(context, TLS_ENABLED, false));
    tls.setVerifyHost(bool(context, TLS_VERIFY_HOST, true));
    tls.setTrustAll(bool(context, TLS_TRUST_ALL, false));
    tls.setEnabledProtocols(INTERNAL_TLS_PROTOCOLS);
    if (sslContextService != null) {
      applySslContextService(tls, sslContextService);
    }

    final String username = blankToNull(context.getProperty(USERNAME).getValue());
    final String password = blankToNull(context.getProperty(PASSWORD).getValue());

    final AmqpExtensionProperties.Sasl sasl = new AmqpExtensionProperties.Sasl();
    sasl.setEnabled(username != null || password != null);
    sasl.setMechanisms(INTERNAL_SASL_MECHANISMS);

    final AmqpExtensionProperties.Settlement settlement = new AmqpExtensionProperties.Settlement();
    settlement.setSuccess(settlement(context, SUCCESS_SETTLEMENT, SettlementMode.ACCEPTED));
    settlement.setFailure(settlement(context, FAILURE_SETTLEMENT, SettlementMode.REJECTED));
    settlement.setRetry(settlement(context, RETRY_SETTLEMENT, SettlementMode.RELEASED));

    final AmqpExtensionProperties.Broker broker = new AmqpExtensionProperties.Broker();
    broker.setHost(context.getProperty(HOST).getValue());
    broker.setPort(integer(context, PORT, 5672));
    broker.setAddress(context.getProperty(ADDRESS).getValue());
    broker.setUsername(username);
    broker.setPassword(password);
    broker.setVirtualHost(blankToNull(context.getProperty(VIRTUAL_HOST).getValue()));
    broker.setTls(tls);
    broker.setSasl(sasl);
    broker.setSettlement(settlement);

    final AmqpExtensionProperties properties = new AmqpExtensionProperties();
    properties.setDefaults(defaults);
    properties.setBrokers(Map.of(DEFAULT_PROFILE, broker));
    properties.validate();
    return properties;
  }

  private static PropertyDescriptor.Builder descriptor(
      final String name, final String description) {
    return new PropertyDescriptor.Builder()
        .name(name)
        .description(description)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE);
  }

  private static PropertyDescriptor settlementDescriptor(
      final String name, final String description, final String defaultValue) {
    return descriptor(name, description)
        .required(true)
        .defaultValue(defaultValue)
        .allowableValues(
            Arrays.stream(SettlementMode.values()).map(Enum::name).toArray(String[]::new))
        .build();
  }

  private static void applySslContextService(
      final AmqpExtensionProperties.Tls tls, final SSLContextService sslContextService) {
    if (sslContextService.isKeyStoreConfigured()) {
      tls.setKeyStoreLocation(blankToNull(sslContextService.getKeyStoreFile()));
      tls.setKeyStorePassword(blankToNull(sslContextService.getKeyStorePassword()));
      tls.setKeyStoreType(blankToNull(sslContextService.getKeyStoreType()));
    }
    if (sslContextService.isTrustStoreConfigured()) {
      tls.setTrustStoreLocation(blankToNull(sslContextService.getTrustStoreFile()));
      tls.setTrustStorePassword(blankToNull(sslContextService.getTrustStorePassword()));
      tls.setTrustStoreType(blankToNull(sslContextService.getTrustStoreType()));
    }
  }

  private static String blankToNull(final String value) {
    return value == null || value.isBlank() ? null : value;
  }

  private static int integer(
      final ProcessContext context, final PropertyDescriptor descriptor, final int fallback) {
    final Integer value = context.getProperty(descriptor).asInteger();
    return value == null ? fallback : value;
  }

  private static boolean bool(
      final ProcessContext context, final PropertyDescriptor descriptor, final boolean fallback) {
    final Boolean value = context.getProperty(descriptor).asBoolean();
    return value == null ? fallback : value;
  }

  private static Duration duration(
      final ProcessContext context, final PropertyDescriptor descriptor, final Duration fallback) {
    final Long millis = context.getProperty(descriptor).asTimePeriod(TimeUnit.MILLISECONDS);
    return millis == null ? fallback : Duration.ofMillis(millis);
  }

  private static SettlementMode settlement(
      final ProcessContext context,
      final PropertyDescriptor descriptor,
      final SettlementMode fallback) {
    final String configured = context.getProperty(descriptor).getValue();
    return configured == null || configured.isBlank()
        ? fallback
        : SettlementMode.valueOf(configured.trim());
  }
}
