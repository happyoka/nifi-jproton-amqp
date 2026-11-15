package com.swim.nifi.amqp.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.swim.nifi.amqp.exception.RetryableAmqpException;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.Tracker;
import org.junit.jupiter.api.Test;

/** Tests behavior of the AMQP publisher and its retry logic. */
class AmqpPublisherTest {

  private static ProcessSession processSession(final byte[] payload) {
    return (ProcessSession)
        Proxy.newProxyInstance(
            ProcessSession.class.getClassLoader(),
            new Class<?>[] {ProcessSession.class},
            (proxy, method, args) -> {
                if (method.getName().equals("read")) {
                    final InputStreamCallback callback = (InputStreamCallback) args[1];
                    callback.process(new ByteArrayInputStream(payload));
                    return null;
                }
                return defaultValue(method.getReturnType());
            });
  }

  private static FlowFile flowFile(final Map<String, String> attributes, final long size) {
    return (FlowFile)
        Proxy.newProxyInstance(
            FlowFile.class.getClassLoader(),
            new Class<?>[] {FlowFile.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "getAttributes" -> attributes;
                case "getAttribute" -> attributes.get(args[0]);
                case "getSize" -> size;
                default -> defaultValue(method.getReturnType());
            });
  }

  private static Connection connectionProxy(final Session session) {
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "openFuture" -> new CompletableFuture<Connection>();
                case "openSession" -> session;
                case "openAnonymousSender" -> null;
                default -> defaultValue(method.getReturnType());
            });
  }

  private static Session sessionProxy(final Sender sender) {
    return (Session)
        Proxy.newProxyInstance(
            Session.class.getClassLoader(),
            new Class<?>[] {Session.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "openFuture" -> new CompletableFuture<Session>();
                case "openAnonymousSender" -> sender;
                case "close" -> null;
                default -> defaultValue(method.getReturnType());
            });
  }

  private static Sender senderProxy(final SendAction sendAction) {
    return (Sender)
        Proxy.newProxyInstance(
            Sender.class.getClassLoader(),
            new Class<?>[] {Sender.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "send" -> {
                    if (args.length == 1) {
                        yield sendAction.send((Message<?>) args[0], null);
                    }
                    yield sendAction.send((Message<?>) args[0], (Map<String, Object>) args[1]);
                }
                case "close" -> null;
                case "trySend" -> sendAction.send(
                        (Message<?>) args[0], args.length > 1 ? (Map<String, Object>) args[1] : null);
                default -> defaultValue(method.getReturnType());
            });
  }

  private static Tracker trackerProxy() {
    return (Tracker)
        Proxy.newProxyInstance(
            Tracker.class.getClassLoader(),
            new Class<?>[] {Tracker.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "awaitAccepted" -> proxy;
                case "awaitSettlement" -> proxy;
                case "sender" -> null;
                default -> defaultValue(method.getReturnType());
            });
  }

  private static ComponentLog componentLogProxy() {
    return (ComponentLog)
        Proxy.newProxyInstance(
            ComponentLog.class.getClassLoader(),
            new Class<?>[] {ComponentLog.class},
            (proxy, method, args) -> defaultValue(method.getReturnType()));
  }

  private static Object defaultValue(final Class<?> returnType) {
    if (returnType == Void.TYPE) {
      return null;
    }
    if (returnType == Boolean.TYPE) {
      return false;
    }
    if (returnType == Byte.TYPE) {
      return (byte) 0;
    }
    if (returnType == Short.TYPE) {
      return (short) 0;
    }
    if (returnType == Integer.TYPE) {
      return 0;
    }
    if (returnType == Long.TYPE) {
      return 0L;
    }
    if (returnType == Float.TYPE) {
      return 0F;
    }
    if (returnType == Double.TYPE) {
      return 0D;
    }
    if (returnType == Character.TYPE) {
      return '\0';
    }
    return null;
  }

  @Test
  void shouldPublishFlowFilePayloadToTopicAddressWithMappedMetadata() throws Exception {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("amqp.payload-kind", "TEXT");
    attributes.put("amqp.content-type", "text/plain");
    attributes.put("amqp.message-id", "msg-42");
    attributes.put("amqp.subject", "orders.created");
    attributes.put("amqp.application.tenant", "acme");

    final byte[] payload = "hello world".getBytes();
    final FlowFile flowFile = flowFile(attributes, payload.length);
    final Message<?>[] capturedMessage = new Message<?>[1];
    final Sender sender =
        senderProxy(
            (message, annotations) -> {
              capturedMessage[0] = message;
              return trackerProxy();
            });
    final Session session = sessionProxy(sender);
    final Connection connection = connectionProxy(session);
    final ComponentLog log = componentLogProxy();

    final AmqpPublisher publisher = new AmqpPublisher(connection, true, 500L, log);
    publisher.publish(flowFile, processSession(payload), "orders");

    assertEquals("topic://orders", capturedMessage[0].to());
    assertEquals("msg-42", capturedMessage[0].messageId());
    assertEquals("orders.created", capturedMessage[0].subject());
    assertEquals("text/plain", capturedMessage[0].contentType());
    assertEquals("acme", capturedMessage[0].property("tenant"));
    assertEquals("hello world", capturedMessage[0].body());
  }

  @Test
  void shouldWrapPublishFailureAsRetryableAmqpException() throws Exception {
    final FlowFile flowFile = flowFile(Map.of(), 5L);
    final Sender sender =
        senderProxy(
            (message, annotations) -> {
              throw new RuntimeException("send failed");
            });
    final Session session = sessionProxy(sender);
    final Connection connection = connectionProxy(session);
    final ComponentLog log = componentLogProxy();

    final AmqpPublisher publisher = new AmqpPublisher(connection, false, 250L, log);

    final RetryableAmqpException exception =
        assertThrows(
            RetryableAmqpException.class,
            () -> publisher.publish(flowFile, processSession("boom".getBytes()), "abcd"));

    assertEquals("Failed to publish message", exception.getMessage());
    assertInstanceOf(RuntimeException.class, exception.getCause());
  }

  @FunctionalInterface
  private interface SendAction {
    Tracker send(Message<?> message, Map<String, Object> annotations);
  }
}
