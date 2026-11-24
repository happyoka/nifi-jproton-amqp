package com.swim.nifi.amqp.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.swim.nifi.amqp.model.ConsumedMessage;
import com.swim.nifi.amqp.model.SettlementMode;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DeferredAcknowledgementRegistryTest {

  @Test
  void shouldSettleAcceptedWhenMarkedAsSuccess() {
    final AtomicReference<SettlementMode> settledMode = new AtomicReference<>();
    final ConsumedMessage consumedMessage =
        new ConsumedMessage(
            new byte[0],
            Map.of(),
            settledMode::set,
            SettlementMode.ACCEPTED,
            SettlementMode.RELEASED);

    final String token = DeferredAcknowledgementRegistry.register(consumedMessage);

    assertTrue(DeferredAcknowledgementRegistry.settleSuccess(token));
    assertEquals(SettlementMode.ACCEPTED, settledMode.get());
    assertFalse(DeferredAcknowledgementRegistry.settleSuccess(token));
  }

  @Test
  void shouldSettleReleasedWhenMarkedAsRetry() {
    final AtomicReference<SettlementMode> settledMode = new AtomicReference<>();
    final ConsumedMessage consumedMessage =
        new ConsumedMessage(
            new byte[0],
            Map.of(),
            settledMode::set,
            SettlementMode.ACCEPTED,
            SettlementMode.RELEASED);

    final String token = DeferredAcknowledgementRegistry.register(consumedMessage);

    assertTrue(DeferredAcknowledgementRegistry.settleRetry(token));
    assertEquals(SettlementMode.RELEASED, settledMode.get());
    assertFalse(DeferredAcknowledgementRegistry.settleRetry("missing-token"));
  }
}
