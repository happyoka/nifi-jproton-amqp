package com.swim.nifi.amqp.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.swim.nifi.amqp.client.AmqpClient;
import com.swim.nifi.amqp.config.AmqpExtensionProperties;
import com.swim.nifi.amqp.exception.RetryableAmqpException;
import com.swim.nifi.amqp.handler.ConsumeMessageHandler;
import com.swim.nifi.amqp.handler.PublishMessageHandler;
import com.swim.nifi.amqp.mapping.FlowFileAttributeMapper;
import com.swim.nifi.amqp.model.ConsumedMessage;
import com.swim.nifi.amqp.model.OutboundMessage;
import com.swim.nifi.amqp.model.SettlementMode;
import com.swim.nifi.amqp.runtime.ProcessorRuntime;
import com.swim.nifi.amqp.support.ExecutionBoundController;
import com.swim.nifi.amqp.support.PayloadGenerator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509TrustManager;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

/** Processor tests for AMQP publish routing and metadata handling. */
class PublishAMQP_1_0Test {
  /** Base stub client for tests that implements all methods with no-ops. */
  abstract static class StubAmqpClient implements AmqpClient {
    @Override
    public void publish(final String profileName, final OutboundMessage outboundMessage) {}

    @Override
    public void subscribe(final String profileName, final String addressOverride) {}

    @Override
    public Optional<ConsumedMessage> poll(final String profileName) {
      return Optional.empty();
    }

    @Override
    public void unsubscribe(final String profileName) {}

    @Override
    public void close() {}
  }

  /**
   * Creates a lightweight processor runtime for tests.
   *
   * @param client client stub used by handlers
   * @return test runtime
   */
  private static ProcessorRuntime runtime(final AmqpClient client) {
    final FlowFileAttributeMapper mapper =
        new FlowFileAttributeMapper(new com.fasterxml.jackson.databind.ObjectMapper());
    final PublishMessageHandler publishMessageHandler =
        new PublishMessageHandler(
            new ExecutionBoundController(new AmqpExtensionProperties(), Runnable::run),
            client,
            mapper,
            new PayloadGenerator());
    final ConsumeMessageHandler consumeMessageHandler =
        new ConsumeMessageHandler(
            new ExecutionBoundController(new AmqpExtensionProperties(), Runnable::run), client);
    return new ProcessorRuntime() {
      @Override
      public PublishMessageHandler publishHandler() {
        return publishMessageHandler;
      }

      @Override
      public ConsumeMessageHandler consumeHandler() {
        return consumeMessageHandler;
      }

      @Override
      public void close() {}
    };
  }

  /** Verifies that a successful publish routes the FlowFile to success. */
  @Test
  void shouldRoutePublishedFlowFileToSuccess() {
    final AtomicReference<OutboundMessage> captured = new AtomicReference<>();
    final AtomicReference<AmqpExtensionProperties> capturedProperties = new AtomicReference<>();
    final TestRunner runner =
        TestRunners.newTestRunner(
            new PublishAMQP_1_0(
                properties -> {
                  capturedProperties.set(properties);
                  return runtime(
                      new StubAmqpClient() {
                        @Override
                        public void publish(
                            final String profileName, final OutboundMessage outboundMessage) {
                          captured.set(outboundMessage);
                        }
                      });
                }));
    runner.enqueue(
        "hello".getBytes(),
        Map.of("amqp.subject", "subject-1", "amqp.application.tenant", "acme"));
    runner.run();
    runner.assertTransferCount(BaseAmqpProcessor.REL_SUCCESS, 1);
    assertEquals("subject-1", captured.get().subject());
    assertEquals("acme", captured.get().applicationProperties().get("tenant"));
    assertNotNull(capturedProperties.get());
    assertEquals(
        "localhost",
        capturedProperties.get().getRequiredBroker(BaseAmqpProcessor.DEFAULT_PROFILE).getHost());
    assertEquals(
        "queue://example",
        capturedProperties
            .get()
            .getRequiredBroker(BaseAmqpProcessor.DEFAULT_PROFILE)
            .getAddress());
  }

  @Test
  void shouldCreateRuntimePropertiesFromPropertyDescriptors() {
    final AtomicReference<AmqpExtensionProperties> capturedProperties = new AtomicReference<>();
    final TestRunner runner =
        TestRunners.newTestRunner(
            new PublishAMQP_1_0(
                properties -> {
                  capturedProperties.set(properties);
                  return runtime(
                      new StubAmqpClient() {
                        @Override
                        public void publish(
                            final String profileName, final OutboundMessage outboundMessage) {}
                      });
                }));
    runner.setProperty("Host", "broker.example.com");
    runner.setProperty("Port", "5671");
    runner.setProperty("Address", "topic://orders");
    runner.setProperty("Username", "alice");
    runner.setProperty("Password", "secret");
    runner.setProperty("Virtual Host", "/tenant-a");
    runner.setProperty("Max Concurrent Operations", "12");
    runner.setProperty("Send Timeout", "30 sec");
    runner.setProperty("Receive Timeout", "12 sec");
    runner.setProperty("Commit Timeout", "45 sec");
    runner.setProperty("Credit Window", "0");
    runner.setProperty("Reconnect Enabled", "false");
    runner.setProperty("Trace Frames", "true");
    runner.setProperty("TLS Enabled", "true");
    runner.setProperty("TLS Verify Host", "false");
    runner.setProperty("TLS Trust All", "true");
    runner.setProperty("Success Settlement", "RELEASED");
    runner.setProperty("Failure Settlement", "ACCEPTED");
    runner.setProperty("Retry Settlement", "REJECTED");
    runner.enqueue("hello".getBytes());
    runner.run();
    final AmqpExtensionProperties properties = capturedProperties.get();
    assertNotNull(properties);
    assertEquals(12, properties.getDefaults().getMaxConcurrentOperations());
    assertEquals(30, properties.getDefaults().getSendTimeout().toSeconds());
    assertEquals(12, properties.getDefaults().getReceiveTimeout().toSeconds());
    assertEquals(45, properties.getDefaults().getCommitTimeout().toSeconds());
    assertEquals(0, properties.getDefaults().getCreditWindow());
    assertFalse(properties.getDefaults().isReconnectEnabled());
    assertTrue(properties.getDefaults().isTraceFrames());
    final AmqpExtensionProperties.Broker broker =
        properties.getRequiredBroker(BaseAmqpProcessor.DEFAULT_PROFILE);
    assertEquals("broker.example.com", broker.getHost());
    assertEquals(5671, broker.getPort());
    assertEquals("topic://orders", broker.getAddress());
    assertEquals("alice", broker.getUsername());
    assertEquals("secret", broker.getPassword());
    assertEquals("/tenant-a", broker.getVirtualHost());
    assertTrue(broker.getTls().isEnabled());
    assertFalse(broker.getTls().isVerifyHost());
    assertTrue(broker.getTls().isTrustAll());
    assertEquals(List.of("TLSv1.3", "TLSv1.2"), broker.getTls().getEnabledProtocols());
    assertNull(broker.getTls().getKeyStoreLocation());
    assertNull(broker.getTls().getTrustStoreLocation());
    assertTrue(broker.getSasl().isEnabled());
    assertEquals(List.of("PLAIN"), broker.getSasl().getMechanisms());
    assertEquals(SettlementMode.RELEASED, broker.getSettlement().getSuccess());
    assertEquals(SettlementMode.ACCEPTED, broker.getSettlement().getFailure());
    assertEquals(SettlementMode.REJECTED, broker.getSettlement().getRetry());
  }

  @Test
  void shouldCreateRuntimeTlsPropertiesFromSslContextService() throws InitializationException {
    final AtomicReference<AmqpExtensionProperties> capturedProperties = new AtomicReference<>();
    final TestRunner runner =
        TestRunners.newTestRunner(
            new PublishAMQP_1_0(
                properties -> {
                  capturedProperties.set(properties);
                  return runtime(
                      new StubAmqpClient() {
                        @Override
                        public void publish(
                            final String profileName, final OutboundMessage outboundMessage) {}
                      });
                }));
    final StubSslContextService sslContextService =
        new StubSslContextService(
            "C:/certs/service-keystore.p12",
            "changeit",
            "PKCS12",
            "C:/certs/service-truststore.p12",
            "trustit",
            "JKS");
    runner.addControllerService("ssl-service", sslContextService);
    runner.enableControllerService(sslContextService);
    runner.setProperty(AmqpProcessorPropertySupport.SSL_CONTEXT_SERVICE, "ssl-service");
    runner.setProperty("TLS Enabled", "false");
    runner.setProperty("TLS Verify Host", "false");
    runner.enqueue("hello".getBytes());
    runner.run();

    final AmqpExtensionProperties.Tls tls =
        capturedProperties
            .get()
            .getRequiredBroker(BaseAmqpProcessor.DEFAULT_PROFILE)
            .getTls();
    assertTrue(tls.isEnabled());
    assertFalse(tls.isVerifyHost());
    assertEquals(List.of("TLSv1.3", "TLSv1.2"), tls.getEnabledProtocols());
    assertEquals("C:/certs/service-keystore.p12", tls.getKeyStoreLocation());
    assertEquals("changeit", tls.getKeyStorePassword());
    assertEquals("PKCS12", tls.getKeyStoreType());
    assertEquals("C:/certs/service-truststore.p12", tls.getTrustStoreLocation());
    assertEquals("trustit", tls.getTrustStorePassword());
    assertEquals("JKS", tls.getTrustStoreType());
    assertNull(tls.getKeyAlias());
  }

  @Test
  void shouldRouteUnreachableBrokerPublishFailureToRetryAndLogProcessorBulletin() {
    final TestRunner runner =
        TestRunners.newTestRunner(
            new PublishAMQP_1_0(
                properties ->
                    runtime(
                        new StubAmqpClient() {
                          @Override
                          public void publish(
                              final String profileName, final OutboundMessage outboundMessage) {
                            throw new RetryableAmqpException(
                                "Failed to open AMQP connection",
                                new IllegalStateException("Connection refused"));
                          }
                        })));
    runner.enqueue("hello".getBytes());
    runner.run();
    runner.assertTransferCount(BaseAmqpProcessor.REL_RETRY, 1);
    runner.assertPenalizeCount(1);
    assertTrue(
        runner.getLogger().getErrorMessages().stream()
            .anyMatch(
                message ->
                    message
                        .getMsg()
                        .contains(
                            "Transient AMQP transport failure while publishing with broker localhost:5672 at queue://example; routing to retry. Cause: Connection refused")));
  }

  @Test
  void shouldRoutePublishTimeoutToRetryAndLogProcessorBulletin() {
    final TestRunner runner =
        TestRunners.newTestRunner(
            new PublishAMQP_1_0(
                properties ->
                    runtime(
                        new StubAmqpClient() {
                          @Override
                          public void publish(
                              final String profileName, final OutboundMessage outboundMessage) {
                            throw new RetryableAmqpException(
                                "Failed to publish AMQP message to queue://example",
                                new TimeoutException("Timed out waiting for remote settlement"));
                          }
                        })));
    runner.enqueue("hello".getBytes());
    runner.run();
    runner.assertTransferCount(BaseAmqpProcessor.REL_RETRY, 1);
    runner.assertPenalizeCount(1);
    assertTrue(
        runner.getLogger().getErrorMessages().stream()
            .anyMatch(
                message ->
                    message
                        .getMsg()
                        .contains(
                            "Transient AMQP transport failure while publishing with broker localhost:5672 at queue://example; routing to retry. Cause: Timed out waiting for remote settlement")));
  }


  private static final class StubSslContextService extends AbstractControllerService
      implements SSLContextService {
    private final String keyStoreFile;
    private final String keyStorePassword;
    private final String keyStoreType;
    private final String trustStoreFile;
    private final String trustStorePassword;
    private final String trustStoreType;

    private StubSslContextService(
        final String keyStoreFile,
        final String keyStorePassword,
        final String keyStoreType,
        final String trustStoreFile,
        final String trustStorePassword,
        final String trustStoreType) {
      this.keyStoreFile = keyStoreFile;
      this.keyStorePassword = keyStorePassword;
      this.keyStoreType = keyStoreType;
      this.trustStoreFile = trustStoreFile;
      this.trustStorePassword = trustStorePassword;
      this.trustStoreType = trustStoreType;
    }

    @Override
    public TlsConfiguration createTlsConfiguration() {
      return null;
    }

    @Override
    public String getTrustStoreFile() {
      return trustStoreFile;
    }

    @Override
    public String getTrustStoreType() {
      return trustStoreType;
    }

    @Override
    public String getTrustStorePassword() {
      return trustStorePassword;
    }

    @Override
    public boolean isTrustStoreConfigured() {
      return trustStoreFile != null;
    }

    @Override
    public String getKeyStoreFile() {
      return keyStoreFile;
    }

    @Override
    public String getKeyStoreType() {
      return keyStoreType;
    }

    @Override
    public String getKeyStorePassword() {
      return keyStorePassword;
    }

    @Override
    public String getKeyPassword() {
      return keyStorePassword;
    }

    @Override
    public boolean isKeyStoreConfigured() {
      return keyStoreFile != null;
    }

    @Override
    public String getSslAlgorithm() {
      return "TLS";
    }

    @Override
    public SSLContext createContext() {
      return null;
    }

    @Override
    public Optional<X509ExtendedKeyManager> createKeyManager() {
      return Optional.empty();
    }

    @Override
    public X509TrustManager createTrustManager() {
      return null;
    }
  }
}
