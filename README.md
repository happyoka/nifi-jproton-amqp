NiFi AMQP 1.0 Extension This project provides a custom Apache NiFi Archive (NAR) with two processors:
- `PublishAmqp10`: publishes incoming FlowFile content to AMQP 1.0 targets.
- `ConsumeAmqp10`: consumes AMQP 1.0 messages and emits them as FlowFiles.
## Stack
- Java 21
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
