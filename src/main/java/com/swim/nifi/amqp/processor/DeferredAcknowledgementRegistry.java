package com.swim.nifi.amqp.processor;

import com.swim.nifi.amqp.model.AmqpAcknowledgement;
import com.swim.nifi.amqp.model.ConsumedMessage;
import com.swim.nifi.amqp.model.SettlementMode;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Thread-safe in-memory registry that holds deferred AMQP acknowledgement handles so downstream
 * publish processing can settle source-broker deliveries after an outbound send completes.
 *
 * <p>Each registered delivery is assigned a unique correlation token stored on the corresponding
 * NiFi FlowFile under {@link #DEFERRED_ACK_TOKEN_ATTRIBUTE}. Entries older than {@link
 * #ENTRY_TTL_MILLIS} are pruned during registration to prevent unbounded memory growth if a
 * FlowFile is dropped, lost, or never reaches settlement.
 *
 * <p><strong>Note:</strong> Because state is kept in JVM memory, pending deliveries do not survive
 * JVM restarts or cross-node cluster handoffs without additional coordination.
 */
final class DeferredAcknowledgementRegistry {

  /** FlowFile attribute name used to correlate a FlowFile with its registered pending delivery. */
  static final String DEFERRED_ACK_TOKEN_ATTRIBUTE = "amqp.deferred-ack.token";

  /** Active pending acknowledgements keyed by generated correlation token. */
  private static final ConcurrentMap<String, PendingAcknowledgement> PENDING =
      new ConcurrentHashMap<>();

  /** Maximum age in milliseconds before a pending entry is eligible for expiration pruning. */
  private static final long ENTRY_TTL_MILLIS = TimeUnit.MINUTES.toMillis(30);

  /** Prevents instantiation of this utility registry. */
  private DeferredAcknowledgementRegistry() {}

  /**
   * Prunes expired entries, creates a new correlation token, and stores the delivery's
   * acknowledgement handle and settlement modes for later completion.
   *
   * @param consumedMessage the consumed AMQP message containing the acknowledgement handle and
   *     configured success/retry settlement modes
   * @return a unique correlation token identifying the registered pending acknowledgement
   */
  static String register(final ConsumedMessage consumedMessage) {
    pruneExpiredEntries();
    final String token = UUID.randomUUID().toString();
    PENDING.put(
        token,
        new PendingAcknowledgement(
            consumedMessage.acknowledgement(),
            consumedMessage.successSettlement(),
            consumedMessage.retrySettlement(),
            System.currentTimeMillis()));
    return token;
  }

  /**
   * Removes and settles the pending acknowledgement associated with {@code token} using its
   * configured success settlement mode.
   *
   * @param token the correlation token assigned to the FlowFile
   * @return {@code true} if a matching pending entry was found and settled; {@code false} if the
   *     token is null, blank, already settled, or expired
   */
  static boolean settleSuccess(final String token) {
    return settle(token, true);
  }

  /**
   * Removes and settles the pending acknowledgement associated with {@code token} using its
   * configured retry settlement mode.
   *
   * @param token the correlation token assigned to the FlowFile
   * @return {@code true} if a matching pending entry was found and settled; {@code false} if the
   *     token is null, blank, already settled, or expired
   */
  static boolean settleRetry(final String token) {
    return settle(token, false);
  }

  /**
   * Looks up, removes, and settles a pending acknowledgement for the given correlation token.
   *
   * @param token the correlation token to resolve
   * @param success {@code true} to apply {@link PendingAcknowledgement#successSettlement()}, or
   *     {@code false} to apply {@link PendingAcknowledgement#retrySettlement()}
   * @return {@code true} if a pending entry existed and was settled; {@code false} otherwise
   */
  private static boolean settle(final String token, final boolean success) {
    if (token == null || token.isBlank()) {
      return false;
    }
    final PendingAcknowledgement pending = PENDING.remove(token);
    if (pending == null) {
      return false;
    }
    pending
        .acknowledgement()
        .settle(success ? pending.successSettlement() : pending.retrySettlement());
    return true;
  }

  /** Evicts entries from {@link #PENDING} whose elapsed age exceeds {@link #ENTRY_TTL_MILLIS}. */
  static int pruneExpiredEntries() {
    final int before = PENDING.size();
    final long now = System.currentTimeMillis();
    PENDING.entrySet().removeIf(entry -> now - entry.getValue().createdAt() > ENTRY_TTL_MILLIS);
    return Math.max(0, before - PENDING.size());
  }

  /**
   * Settles all currently pending entries using retry settlement and clears the registry.
   *
   * @return number of pending entries removed from the registry
   */
  static int settleAllRetry() {
    int removed = 0;
    for (final var entry : PENDING.entrySet()) {
      final PendingAcknowledgement pending = PENDING.remove(entry.getKey());
      if (pending == null) {
        continue;
      }
      try {
        pending.acknowledgement().settle(pending.retrySettlement());
      } catch (final Exception ignored) {
        // Best-effort cleanup during shutdown; remove entry even if settlement call fails.
      }
      removed++;
    }
    return removed;
  }

  /**
   * Immutable snapshot of an unsettled inbound AMQP delivery awaiting post-publish settlement.
   *
   * @param acknowledgement handle used to send the AMQP disposition/settlement to the source broker
   * @param successSettlement settlement mode to apply when downstream publish succeeds
   * @param retrySettlement settlement mode to apply when downstream publish fails or requires retry
   * @param createdAt epoch timestamp in milliseconds when the entry was registered
   */
  private record PendingAcknowledgement(
      AmqpAcknowledgement acknowledgement,
      SettlementMode successSettlement,
      SettlementMode retrySettlement,
      long createdAt) {}
}
