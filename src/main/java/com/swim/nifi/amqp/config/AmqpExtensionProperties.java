package com.swim.nifi.amqp.config;

import com.swim.nifi.amqp.model.SettlementMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Runtime configuration model for the NiFi AMQP extension.
 *
 * <p>The model remains profile-based so processors can resolve a named broker definition while the
 * surrounding runtime decides how those values are supplied.
 */
public class AmqpExtensionProperties {

  private Defaults defaults = new Defaults();
  private Map<String, Broker> brokers = new LinkedHashMap<>();

  private static void validateBroker(final String profileName, final Broker broker) {
    if (broker == null) {
      throw new IllegalArgumentException("Broker profile '" + profileName + "' must not be null");
    }
    if (broker.host == null || broker.host.isBlank()) {
      throw new IllegalArgumentException(
          "Broker profile '" + profileName + "' must define a non-blank host");
    }
    if (broker.port < 1) {
      throw new IllegalArgumentException(
          "Broker profile '" + profileName + "' must define a port greater than zero");
    }
    if (broker.address == null || broker.address.isBlank()) {
      throw new IllegalArgumentException(
          "Broker profile '" + profileName + "' must define a non-blank address");
    }

    if (broker.tls == null) {
      broker.tls = new Tls();
    }
    if (broker.sasl == null) {
      broker.sasl = new Sasl();
    }
    if (broker.settlement == null) {
      broker.settlement = new Settlement();
    }
    if (broker.tls.enabledProtocols == null || broker.tls.enabledProtocols.isEmpty()) {
      broker.tls.enabledProtocols = new ArrayList<>(List.of("TLSv1.3", "TLSv1.2"));
    }
    if (broker.sasl.mechanisms == null || broker.sasl.mechanisms.isEmpty()) {
      broker.sasl.mechanisms = new ArrayList<>(List.of("PLAIN"));
    }
  }

  /**
   * Returns default execution and transport bounds applied when a broker profile omits a value.
   *
   * @return extension defaults
   */
  public Defaults getDefaults() {
    return defaults;
  }

  /**
   * Replaces the extension defaults section.
   *
   * @param defaults defaults to apply across processor instances
   */
  public void setDefaults(final Defaults defaults) {
    this.defaults = defaults;
  }

  /**
   * Returns configured broker profiles keyed by profile name.
   *
   * @return broker profiles
   */
  public Map<String, Broker> getBrokers() {
    return brokers;
  }

  /**
   * Replaces the configured broker profile map.
   *
   * @param brokers broker profiles keyed by user-facing profile name
   */
  public void setBrokers(final Map<String, Broker> brokers) {
    this.brokers = brokers;
  }

  /**
   * Resolves a named broker profile or throws an informative exception when the configuration is
   * incomplete.
   *
   * @param profileName logical profile name selected by the processor
   * @return resolved broker configuration
   * @throws IllegalArgumentException when the profile does not exist
   */
  public Broker getRequiredBroker(final String profileName) {
    final Broker broker = brokers.get(profileName);
    if (broker == null) {
      throw new IllegalArgumentException(
          "No broker profile named '" + profileName + "' is configured");
    }
    validateBroker(profileName, broker);
    return broker;
  }

  /** Validates the loaded properties and normalizes optional nested structures. */
  public void validate() {
    if (defaults == null) {
      defaults = new Defaults();
    }
    defaults.validate();

    if (brokers == null || brokers.isEmpty()) {
      throw new IllegalArgumentException(
          "At least one broker profile must be configured under nifi.amqp.brokers");
    }
    brokers.forEach(AmqpExtensionProperties::validateBroker);
  }

  /** Default transport and execution bounds shared across broker profiles. */
  public static class Defaults {

    private int maxConcurrentOperations = 64;

    private Duration sendTimeout = Duration.ofSeconds(15);
    private Duration receiveTimeout = Duration.ofSeconds(5);
    private Duration commitTimeout = Duration.ofSeconds(30);

    private int creditWindow = 25;

    private boolean reconnectEnabled = true;
    private boolean traceFrames;

    /**
     * Returns the upper bound for concurrent broker operations per processor context.
     *
     * @return concurrency limit
     */
    public int getMaxConcurrentOperations() {
      return maxConcurrentOperations;
    }

    /**
     * Sets the upper bound for concurrent broker operations per processor context.
     *
     * @param maxConcurrentOperations concurrency limit, where values above one allow parallel
     *     operations on virtual threads
     */
    public void setMaxConcurrentOperations(final int maxConcurrentOperations) {
      this.maxConcurrentOperations = maxConcurrentOperations;
    }

    /**
     * Returns the send timeout used while waiting for remote delivery outcomes.
     *
     * @return send timeout
     */
    public Duration getSendTimeout() {
      return sendTimeout;
    }

    /**
     * Sets the send timeout used while waiting for remote delivery outcomes.
     *
     * @param sendTimeout timeout for sender settlement waiting
     */
    public void setSendTimeout(final Duration sendTimeout) {
      this.sendTimeout = sendTimeout;
    }

    /**
     * Returns the receive timeout used to poll a receiver for incoming deliveries.
     *
     * @return receive timeout
     */
    public Duration getReceiveTimeout() {
      return receiveTimeout;
    }

    /**
     * Sets the receive timeout used to poll a receiver for incoming deliveries.
     *
     * @param receiveTimeout maximum wait for a delivery before yielding
     */
    public void setReceiveTimeout(final Duration receiveTimeout) {
      this.receiveTimeout = receiveTimeout;
    }

    /**
     * Returns the timeout reserved for repository commit completion callbacks.
     *
     * @return commit timeout
     */
    public Duration getCommitTimeout() {
      return commitTimeout;
    }

    /**
     * Sets the timeout reserved for repository commit completion callbacks.
     *
     * @param commitTimeout commit callback budget
     */
    public void setCommitTimeout(final Duration commitTimeout) {
      this.commitTimeout = commitTimeout;
    }

    /**
     * Returns the default receiver credit window.
     *
     * @return receiver credit window, where zero enables manual credit control
     */
    public int getCreditWindow() {
      return creditWindow;
    }

    /**
     * Sets the default receiver credit window.
     *
     * @param creditWindow receiver credit window
     */
    public void setCreditWindow(final int creditWindow) {
      this.creditWindow = creditWindow;
    }

    /**
     * Returns whether transport reconnection is enabled.
     *
     * @return {@code true} when the client may reconnect after interruption
     */
    public boolean isReconnectEnabled() {
      return reconnectEnabled;
    }

    /**
     * Sets whether transport reconnection is enabled.
     *
     * @param reconnectEnabled reconnection toggle
     */
    public void setReconnectEnabled(final boolean reconnectEnabled) {
      this.reconnectEnabled = reconnectEnabled;
    }

    /**
     * Returns whether AMQP frame tracing is enabled.
     *
     * @return {@code true} when frame tracing is enabled for troubleshooting
     */
    public boolean isTraceFrames() {
      return traceFrames;
    }

    /**
     * Sets whether AMQP frame tracing is enabled.
     *
     * @param traceFrames tracing toggle for low-level AMQP diagnostics
     */
    public void setTraceFrames(final boolean traceFrames) {
      this.traceFrames = traceFrames;
    }

    private void validate() {
      if (maxConcurrentOperations < 1) {
        throw new IllegalArgumentException(
            "nifi.amqp.defaults.max-concurrent-operations must be greater than zero");
      }
      if (creditWindow < 0) {
        throw new IllegalArgumentException(
            "nifi.amqp.defaults.credit-window must be zero or greater");
      }
      if (sendTimeout == null) {
        sendTimeout = Duration.ofSeconds(15);
      }
      if (receiveTimeout == null) {
        receiveTimeout = Duration.ofSeconds(5);
      }
      if (commitTimeout == null) {
        commitTimeout = Duration.ofSeconds(30);
      }
    }
  }

  /**
   * Broker profile that defines connection, authentication, and settlement policy for a remote AMQP
   * 1.0 target.
   */
  public static class Broker {

    private String host = "localhost";

    private int port = 5672;

    private String address = "queue://example";

    private String username;
    private String password;
    private String virtualHost;
    private Tls tls = new Tls();
    private Sasl sasl = new Sasl();
    private Settlement settlement = new Settlement();

    /**
     * Returns the broker host name or IP address.
     *
     * @return broker host
     */
    public String getHost() {
      return host;
    }

    /**
     * Sets the broker host name or IP address.
     *
     * @param host broker host name or IP address
     */
    public void setHost(final String host) {
      this.host = host;
    }

    /**
     * Returns the broker port.
     *
     * @return broker port
     */
    public int getPort() {
      return port;
    }

    /**
     * Sets the broker port.
     *
     * @param port broker port, typically 5672 or 5671 for TLS
     */
    public void setPort(final int port) {
      this.port = port;
    }

    /**
     * Returns the default target address used when processors do not override the destination.
     *
     * @return default AMQP node address
     */
    public String getAddress() {
      return address;
    }

    /**
     * Sets the default target address used when processors do not override the destination.
     *
     * @param address AMQP queue, topic, or node address
     */
    public void setAddress(final String address) {
      this.address = address;
    }

    /**
     * Returns the SASL user name.
     *
     * @return configured user name or {@code null} when the selected mechanism does not require one
     */
    public String getUsername() {
      return username;
    }

    /**
     * Sets the SASL user name.
     *
     * @param username user name for broker authentication
     */
    public void setUsername(final String username) {
      this.username = username;
    }

    /**
     * Returns the SASL password.
     *
     * @return configured password or {@code null}
     */
    public String getPassword() {
      return password;
    }

    /**
     * Sets the SASL password.
     *
     * @param password secret used with password-based SASL mechanisms
     */
    public void setPassword(final String password) {
      this.password = password;
    }

    /**
     * Returns the virtual host to send in the AMQP open frame.
     *
     * @return virtual host, empty string to omit, or {@code null} to reuse the connect host
     */
    public String getVirtualHost() {
      return virtualHost;
    }

    /**
     * Sets the virtual host to send in the AMQP open frame.
     *
     * @param virtualHost virtual host override
     */
    public void setVirtualHost(final String virtualHost) {
      this.virtualHost = virtualHost;
    }

    /**
     * Returns TLS settings for the broker profile.
     *
     * @return TLS settings
     */
    public Tls getTls() {
      return tls;
    }

    /**
     * Replaces TLS settings for the broker profile.
     *
     * @param tls TLS settings, including mTLS key material and trust anchors
     */
    public void setTls(final Tls tls) {
      this.tls = tls;
    }

    /**
     * Returns SASL settings for the broker profile.
     *
     * @return SASL settings
     */
    public Sasl getSasl() {
      return sasl;
    }

    /**
     * Replaces SASL settings for the broker profile.
     *
     * @param sasl SASL settings and allowed mechanism list
     */
    public void setSasl(final Sasl sasl) {
      this.sasl = sasl;
    }

    /**
     * Returns settlement policy used by the consume processor.
     *
     * @return settlement policy
     */
    public Settlement getSettlement() {
      return settlement;
    }

    /**
     * Replaces settlement policy used by the consume processor.
     *
     * @param settlement settlement policy for commit success, parse failure, and retry paths
     */
    public void setSettlement(final Settlement settlement) {
      this.settlement = settlement;
    }
  }

  /** TLS and mTLS material for a broker profile. */
  public static class Tls {

    private boolean enabled;
    private boolean verifyHost = true;
    private boolean trustAll;
    private String keyStoreLocation;
    private String keyStorePassword;
    private String keyStoreType = "PKCS12";
    private String keyAlias;
    private String trustStoreLocation;
    private String trustStorePassword;
    private String trustStoreType = "PKCS12";
    private List<String> enabledProtocols = new ArrayList<>(List.of("TLSv1.3", "TLSv1.2"));

    /**
     * Returns whether TLS is enabled.
     *
     * @return {@code true} when the profile connects over TLS
     */
    public boolean isEnabled() {
      return enabled;
    }

    /**
     * Sets whether TLS is enabled.
     *
     * @param enabled TLS toggle
     */
    public void setEnabled(final boolean enabled) {
      this.enabled = enabled;
    }

    /**
     * Returns whether broker host verification is enabled.
     *
     * @return {@code true} when peer hostnames must match certificates
     */
    public boolean isVerifyHost() {
      return verifyHost;
    }

    /**
     * Sets whether broker host verification is enabled.
     *
     * @param verifyHost peer hostname verification toggle
     */
    public void setVerifyHost(final boolean verifyHost) {
      this.verifyHost = verifyHost;
    }

    /**
     * Returns whether all server certificates should be trusted.
     *
     * @return {@code true} when trust validation is disabled for non-production use
     */
    public boolean isTrustAll() {
      return trustAll;
    }

    /**
     * Sets whether all server certificates should be trusted.
     *
     * @param trustAll trust override toggle
     */
    public void setTrustAll(final boolean trustAll) {
      this.trustAll = trustAll;
    }

    /**
     * Returns the client key store path.
     *
     * @return key store location or {@code null}
     */
    public String getKeyStoreLocation() {
      return keyStoreLocation;
    }

    /**
     * Sets the client key store path.
     *
     * @param keyStoreLocation location of the key store used for mTLS authentication
     */
    public void setKeyStoreLocation(final String keyStoreLocation) {
      this.keyStoreLocation = keyStoreLocation;
    }

    /**
     * Returns the client key store password.
     *
     * @return key store password or {@code null}
     */
    public String getKeyStorePassword() {
      return keyStorePassword;
    }

    /**
     * Sets the client key store password.
     *
     * @param keyStorePassword key store password
     */
    public void setKeyStorePassword(final String keyStorePassword) {
      this.keyStorePassword = keyStorePassword;
    }

    /**
     * Returns the client key store format.
     *
     * @return key store type such as PKCS12 or JKS
     */
    public String getKeyStoreType() {
      return keyStoreType;
    }

    /**
     * Sets the client key store format.
     *
     * @param keyStoreType key store type
     */
    public void setKeyStoreType(final String keyStoreType) {
      this.keyStoreType = keyStoreType;
    }

    /**
     * Returns the client certificate alias to select from the key store.
     *
     * @return key alias or {@code null}
     */
    public String getKeyAlias() {
      return keyAlias;
    }

    /**
     * Sets the client certificate alias to select from the key store.
     *
     * @param keyAlias key alias used for mTLS authentication
     */
    public void setKeyAlias(final String keyAlias) {
      this.keyAlias = keyAlias;
    }

    /**
     * Returns the trust store path.
     *
     * @return trust store location or {@code null}
     */
    public String getTrustStoreLocation() {
      return trustStoreLocation;
    }

    /**
     * Sets the trust store path.
     *
     * @param trustStoreLocation location of the trust store used to validate broker certificates
     */
    public void setTrustStoreLocation(final String trustStoreLocation) {
      this.trustStoreLocation = trustStoreLocation;
    }

    /**
     * Returns the trust store password.
     *
     * @return trust store password or {@code null}
     */
    public String getTrustStorePassword() {
      return trustStorePassword;
    }

    /**
     * Sets the trust store password.
     *
     * @param trustStorePassword trust store password
     */
    public void setTrustStorePassword(final String trustStorePassword) {
      this.trustStorePassword = trustStorePassword;
    }

    /**
     * Returns the trust store format.
     *
     * @return trust store type
     */
    public String getTrustStoreType() {
      return trustStoreType;
    }

    /**
     * Sets the trust store format.
     *
     * @param trustStoreType trust store type
     */
    public void setTrustStoreType(final String trustStoreType) {
      this.trustStoreType = trustStoreType;
    }

    /**
     * Returns the enabled TLS protocol list.
     *
     * @return enabled protocol names, ordered by preference
     */
    public List<String> getEnabledProtocols() {
      return enabledProtocols;
    }

    /**
     * Replaces the enabled TLS protocol list.
     *
     * @param enabledProtocols enabled protocol names, typically TLSv1.3 and TLSv1.2
     */
    public void setEnabledProtocols(final List<String> enabledProtocols) {
      this.enabledProtocols = enabledProtocols;
    }
  }

  /** SASL policy for a broker profile. */
  public static class Sasl {

    private boolean enabled = true;
    private List<String> mechanisms = new ArrayList<>(List.of("PLAIN"));

    /**
     * Returns whether SASL is enabled.
     *
     * @return {@code true} when SASL negotiation should be attempted
     */
    public boolean isEnabled() {
      return enabled;
    }

    /**
     * Sets whether SASL is enabled.
     *
     * @param enabled SASL toggle
     */
    public void setEnabled(final boolean enabled) {
      this.enabled = enabled;
    }

    /**
     * Returns the ordered list of allowed SASL mechanisms.
     *
     * @return allowed mechanisms such as PLAIN, ANONYMOUS, or EXTERNAL
     */
    public List<String> getMechanisms() {
      return mechanisms;
    }

    /**
     * Replaces the ordered list of allowed SASL mechanisms.
     *
     * @param mechanisms allowed mechanisms in priority order
     */
    public void setMechanisms(final List<String> mechanisms) {
      this.mechanisms = mechanisms;
    }
  }

  /**
   * Settlement policy used when the consume processor determines how to settle a received delivery
   * after NiFi repository outcomes are known.
   */
  public static class Settlement {

    private SettlementMode success = SettlementMode.ACCEPTED;
    private SettlementMode failure = SettlementMode.REJECTED;
    private SettlementMode retry = SettlementMode.RELEASED;

    /**
     * Returns the settlement outcome used after a successful session commit.
     *
     * @return success settlement mode
     */
    public SettlementMode getSuccess() {
      return success;
    }

    /**
     * Sets the settlement outcome used after a successful session commit.
     *
     * @param success settlement mode for successfully emitted FlowFiles
     */
    public void setSuccess(final SettlementMode success) {
      this.success = success;
    }

    /**
     * Returns the settlement outcome used after a non-retryable parse or mapping failure.
     *
     * @return failure settlement mode
     */
    public SettlementMode getFailure() {
      return failure;
    }

    /**
     * Sets the settlement outcome used after a non-retryable parse or mapping failure.
     *
     * @param failure settlement mode for malformed or unsupported messages
     */
    public void setFailure(final SettlementMode failure) {
      this.failure = failure;
    }

    /**
     * Returns the settlement outcome used after retryable broker or repository failures.
     *
     * @return retry settlement mode
     */
    public SettlementMode getRetry() {
      return retry;
    }

    /**
     * Sets the settlement outcome used after retryable broker or repository failures.
     *
     * @param retry settlement mode that asks the broker to re-deliver later when possible
     */
    public void setRetry(final SettlementMode retry) {
      this.retry = retry;
    }
  }
}
