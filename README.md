NiFi AMQP 1.0 Extension This project provides a custom Apache NiFi Archive (NAR) with two processors:
- `PublishAMQP_1_0`: publishes incoming FlowFile content to AMQP 1.0 targets.
- `ConsumeAMQP_1_0`: consumes AMQP 1.0 messages and emits them as FlowFiles.
## Stack
- Java 25
- Gradle
- Apache NiFi 2.12.0 API
- Apache Qpid ProtonJ2 1.3.0
- SLF4J
- JUnit 5 + NiFi mock test harness
## Build
Run:
```bash
gradle build
```
Artifacts:
- `build/libs/nifi-amqp-extension-1.0.0.nar`
The JAR under `build/libs/` is an internal bundle component used by the NAR archive; Apache NiFi should be pointed at
the `.nar` file.
## Configuration Model
Processors are configured directly through NiFi `PropertyDescriptor` definitions. Each processor instance defines its
own AMQP connection and transport settings, including:
- broker connectivity: `Host`, `Port`, `Address`, `Username`, `Password`, `Virtual Host`
- execution bounds: `Max Concurrent Operations`, `Send Timeout`, `Receive Timeout`, `Commit Timeout`, `Credit Window`
- transport behavior: `Reconnect Enabled`, `Trace Frames`
- TLS and mTLS material: `TLS Enabled`, `SSL Context Service`, `TLS Verify Host`, `TLS Trust All`
- consume settlement outcomes: `Success Settlement`, `Failure Settlement`, `Retry Settlement`
  Default descriptor values preserve the same baseline behavior that was previously supplied by the embedded YAML
  defaults.
When `SSL Context Service` is configured, the processors resolve broker key store and trust store settings from NiFi's
standard `SSLContextService`. Processor-level key store, trust store, and key alias properties are intentionally not
exposed; the controller service is the only source of explicit TLS material.
`Username` and `Password` are blank by default. SASL is enabled automatically when credentials are provided. TLS 
protocol selection and SASL mechanisms are kept internal with secure defaults of `TLSv1.3,TLSv1.2` and `PLAIN`.
## FlowFile Attribute Mapping
Message metadata is mapped using these attribute prefixes:
- `amqp.message-id`
- `amqp.correlation-id`
- `amqp.subject`
- `amqp.to`
- `amqp.reply-to`
- `amqp.content-type`
- `amqp.content-encoding`
- `amqp.header.*`
- `amqp.application.*`
- `amqp.delivery-annotation.*`
  Telemetry headers are also bridged in both directions when present:
- `traceparent`
- `tracestate`
- `baggage`
## Relationships
Both processors expose:
- `success`
- `failure`
- `retry`
  Routing rules:
- successful processing goes to `success`
- unsupported or malformed payload / metadata scenarios go to `failure`
- retryable network or broker faults go to `retry`
## Delivery Semantics
The consume processor uses `ProcessSession.commitAsync(...)` callbacks so AMQP settlement happens only after NiFi
confirms the session commit outcome:
- commit success -> configured success settlement, default `ACCEPTED`
- commit failure -> configured retry settlement, default `RELEASED`
- parse failure -> configured failure settlement, default `REJECTED`
  This provides an at-least-once ingestion pattern aligned with NiFi repository commits.
## Notes
- ProtonJ2 uses the Proton engine and non-blocking I/O internally.
- Handler execution is bounded with a semaphore and dispatched on Java 25 virtual threads.
- The current payload path favors correctness and NiFi processor integration. If a broker requires very large-message
  optimization, the transport can be extended to ProtonJ2 stream sender / receiver APIs without changing processor
  contracts.
