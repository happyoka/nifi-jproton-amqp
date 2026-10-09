package com.swim.nifi.amqp.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.swim.nifi.amqp.client.AmqpClient;
import com.swim.nifi.amqp.config.AmqpExtensionProperties;
import com.swim.nifi.amqp.exception.RetryableAmqpException;
import com.swim.nifi.amqp.exception.UnparseableAmqpMessageException;
import com.swim.nifi.amqp.handler.ConsumeMessageHandler;
import com.swim.nifi.amqp.handler.PublishMessageHandler;
import com.swim.nifi.amqp.mapping.FlowFileAttributeMapper;
import com.swim.nifi.amqp.model.ConsumedMessage;
import com.swim.nifi.amqp.model.OutboundMessage;
import com.swim.nifi.amqp.model.SettlementMode;
import com.swim.nifi.amqp.runtime.ProcessorRuntime;
import com.swim.nifi.amqp.support.ExecutionBoundController;
import com.swim.nifi.amqp.support.PayloadGenerator;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

/** Processor tests for long-lived AMQP consume behavior and routing. */
class ConsumeAMQP_1_0Test {
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

  private static ConsumedMessage consumedMessage(
      final String body,
      final Map<String, String> attributes,
      final AtomicReference<SettlementMode> settledMode) {
    return new ConsumedMessage(
        body.getBytes(StandardCharsets.UTF_8),
        attributes,
        settledMode::set,
        SettlementMode.ACCEPTED,
        SettlementMode.RELEASED);
  }

  @Test
  void shouldCreateFlowFileFromConsumedMessage() {
    final AtomicBoolean served = new AtomicBoolean();
    final AtomicReference<SettlementMode> settledMode = new AtomicReference<>();
    final TestRunner runner =
        TestRunners.newTestRunner(
            new ConsumeAMQP_1_0(
                properties ->
                    runtime(
                        new StubAmqpClient() {
                          @Override
                          public Optional<ConsumedMessage> poll(final String profileName) {
                            if (!served.compareAndSet(false, true)) {
                              return Optional.empty();
                            }
                            return Optional.of(
                                consumedMessage(
                                    "hello",
                                    Map.of(
                                        "amqp.subject", "subject-1",
                                        "amqp.application.tenant", "acme"),
                                    settledMode));
                          }
                        })));

    runner.run();

    runner.assertTransferCount(BaseAmqpProcessor.REL_SUCCESS, 1);
    final MockFlowFile flowFile =
        runner.getFlowFilesForRelationship(BaseAmqpProcessor.REL_SUCCESS).get(0);
    flowFile.assertContentEquals("hello");
    flowFile.assertAttributeEquals("amqp.subject", "subject-1");
    flowFile.assertAttributeEquals("amqp.application.tenant", "acme");
    assertEquals(SettlementMode.ACCEPTED, settledMode.get());
  }

  @Test
  void shouldRouteTransientPollFailureToRetry() {
    final TestRunner runner =
        TestRunners.newTestRunner(
            new ConsumeAMQP_1_0(
                properties ->
                    runtime(
                        new StubAmqpClient() {
                          @Override
                          public Optional<ConsumedMessage> poll(final String profileName) {
                            throw new RetryableAmqpException(
                                "Failed to poll AMQP subscription",
                                new IllegalStateException("Connection refused"));
                          }
                        })));

    runner.run();

    runner.assertTransferCount(BaseAmqpProcessor.REL_RETRY, 1);
    final MockFlowFile flowFile =
        runner.getFlowFilesForRelationship(BaseAmqpProcessor.REL_RETRY).get(0);
    assertTrue(
        flowFile.getAttribute("amqp.error")
            .contains("Transient AMQP transport failure while consuming"));
  }

  @Test
  void shouldUnsubscribeFailedSubscriptionAfterRetryablePollFailure() {
    final AtomicInteger subscribeCalls = new AtomicInteger();
    final AtomicInteger unsubscribeCalls = new AtomicInteger();
    final AtomicInteger pollCalls = new AtomicInteger();
    final TestRunner runner =
        TestRunners.newTestRunner(
            new ConsumeAMQP_1_0(
                properties ->
                    runtime(
                        new StubAmqpClient() {
                          @Override
                          public void subscribe(
                              final String profileName, final String addressOverride) {
                            subscribeCalls.incrementAndGet();
                          }

                          @Override
                          public Optional<ConsumedMessage> poll(final String profileName) {
                            pollCalls.incrementAndGet();
                            throw new RetryableAmqpException(
                                "Failed to poll AMQP subscription",
                                new IllegalStateException("The Receiver was explicitly closed"));
                          }

                          @Override
                          public void unsubscribe(final String profileName) {
                            unsubscribeCalls.incrementAndGet();
                          }
                        })));

    runner.run();

    assertEquals(1, subscribeCalls.get());
    assertEquals(1, unsubscribeCalls.get());
    assertEquals(1, pollCalls.get());
    runner.assertTransferCount(BaseAmqpProcessor.REL_RETRY, 1);
  }

  @Test
  void shouldRouteNonRetryablePollFailureToFailure() {
    final TestRunner runner =
        TestRunners.newTestRunner(
            new ConsumeAMQP_1_0(
                properties ->
                    runtime(
                        new StubAmqpClient() {
                          @Override
                          public Optional<ConsumedMessage> poll(final String profileName) {
                            throw new UnparseableAmqpMessageException(
                                "Body decoding failed", new IllegalArgumentException("bad body"));
                          }
                        })));

    runner.run();

    runner.assertTransferCount(BaseAmqpProcessor.REL_FAILURE, 1);
    final MockFlowFile flowFile =
        runner.getFlowFilesForRelationship(BaseAmqpProcessor.REL_FAILURE).get(0);
    assertEquals("Body decoding failed", flowFile.getAttribute("amqp.error"));
  }
}
