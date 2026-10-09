package com.swim.nifi.amqp.client;

import com.swim.nifi.amqp.model.ConsumedMessage;
import com.swim.nifi.amqp.model.OutboundMessage;
import java.util.Optional;

/** Internal transport facade used by the processor handlers. */
public interface AmqpClient extends AutoCloseable {

  /**
   * Publishes the supplied outbound message.
   *
   * @param profileName broker profile name
   * @param outboundMessage outbound message model
   */
  void publish(String profileName, OutboundMessage outboundMessage);

  /**
   * Establishes a long-lived subscription to consume messages efficiently across processor
   * triggers.
   *
   * @param profileName broker profile name
   * @param addressOverride optional address override
   */
  void subscribe(String profileName, String addressOverride);

  /**
   * Polls an active long-lived subscription for the next available buffered message.
   *
   * @param profileName broker profile name
   * @return optional consumed message
   */
  Optional<ConsumedMessage> poll(String profileName);

  /**
   * Closes a long-lived subscription and releases its resources.
   *
   * @param profileName broker profile name
   */
  void unsubscribe(String profileName);

  /** Closes transport resources. */
  @Override
  void close();
}
