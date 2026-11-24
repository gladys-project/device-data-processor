# device-data-processor

Java 25 foundation for Gladys device-data processing, including the user-authored reactive function, OpenAPI-generated models and MapStruct projection. The function validates incoming messages and emits device-data JSON. No database, device-management decisions, watering logic or pump control is implemented. Lombok and SLF4J are available through the Boot-managed dependencies and explicit annotation processing.

## Run and test

Install Java 25. Maven is supplied by the pinned wrapper:

```bash
./mvnw test          # Fast tests: no Docker or running Kafka
./mvnw clean verify  # Fast tests + real Kafka integration tests; Docker required
./mvnw spring-boot:run
```

The default/local application starts independently of Kafka and serves `http://localhost:8081/actuator/health`. Only health and info are exposed. Test configuration uses unreachable broker addresses until integration tests dynamically supply their own container endpoint. Surefire runs `*Test`; Failsafe runs `*IT` during integration-test and verify. Docker unavailability fails integration verification.

`ApplicationTest` exercises the real HTTP endpoint with WebTestClient and StepVerifier on its response publisher. Contract/model tests validate the supplied JSON, required fields, UUIDs, nonnegative timestamps, schema version, device diagnostics and extensible readings from different sensors. Kafka integration tests start `apache/kafka:4.1.0`, wait explicitly for readiness, provision the topic in test code and exercise the actual production string-to-string function, verifying its device key and plain JSON output, rejection through generated constraints, the existing DLQ path and different sensor types. Consumer waits and producer/admin operations are bounded; resources are closed by the test lifecycle.

## Spring Cloud configuration and activation boundary

Shared defaults and deployment-profile configuration are in [application.yml](src/main/resources/application.yml). [application-local.yml](src/main/resources/application-local.yml) selects host-local Kafka addresses. The declared input is:

| Setting | Default |
| --- | --- |
| Input binding | `processDeviceData-in-0` |
| Output binding | `processDeviceData-out-0` |
| Device-data destination | `gladys.device.data.v1` |
| Raw Kafka destination | `gladys.telemetry.raw.v1` |
| Consumer group | `gladys.device-data-processor.v1` |
| Content type | `application/json` |
| Function definition | `processDeviceData` |
| Kafka record key | `deviceId` |

The default/local profile has an empty `spring.cloud.function.definition`, disabled function autodetection and input/output `auto-startup: false`. It creates no Kafka consumer. The **deployed** profile selects the existing `processDeviceData` bean and enables its input and output bindings. Compose selects that profile. Kafka topics must already exist.

The user-authored function exists in every profile, while binding activation remains profile-controlled. Its public signature stays exactly `Function<Flux<Message<String>>, Flux<Message<String>>>`. It parses JSON into `KafkaRawTelemetry`, invokes Jakarta Validation on generated constraints, maps valid input with MapStruct, and emits JSON strings containing the device-data projection. Integration tests run this actual function; no replacement function bean is registered.

A future Telemetry Processor must use a different consumer group to receive the same raw records independently. Device/sensor/soil resolution stays outside this foundation. The web runtime uses Reactor Netty; Spring Kafka and the regular Spring Cloud Kafka binder are asynchronous/event-driven integrations, not natively reactive streams with end-to-end backpressure.

## Raw telemetry message definition

[openapi/kafka-raw-telemetry.yml](src/main/resources/openapi/kafka-raw-telemetry.yml) is a standalone OpenAPI 3.1.2 document matching the MQTT bridge's Kafka raw output. It embeds `TelemetryPayload` and `DeviceError` schemas so this service does not depend on another checkout. Update both services' contracts together when the wire format changes.

Maven generates `KafkaRawTelemetry`, `TelemetryPayload` and `DeviceError` under `target/generated-sources/openapi/` during `generate-sources`; compiled models are included in the executable JAR. No APIs/controllers are generated. [The JSON example](src/main/resources/openapi/examples/kafka-raw-telemetry.json) retains the complete envelope, battery/errors and sensor payload. `observedAt` is Unix epoch milliseconds, raw moisture is an ADC count, and `sensorId` identifies the sensor whose soil is resolved downstream. Multiple sensors per device are supported; `plantId` is optional and no soil ID is invented. The bridge does not duplicate or split the raw observation.

OpenAPI `required`, numeric ranges, string patterns and nested schemas generate Jakarta Validation annotations. The function invokes the Boot-managed `Validator` before mapping; violations enter the existing error/DLQ path. Schema version and UUID formats are represented by generated enum/UUID types. `errors` is required (an empty array is valid); null error entries are rejected using generated type-use `@NotNull` annotations. Validation rules remain in the message definitions, without field-specific checks in the function. Schema-validation tests separately check the full wire contracts.

## Outbound message and MapStruct mapping

[kafka-device-data.yml](src/main/resources/openapi/kafka-device-data.yml) separately defines the generated `KafkaDeviceData` output. Its `DeviceError` schema references the inbound document so diagnostics share one definition. [The outbound example](src/main/resources/openapi/examples/kafka-device-data.json) preserves `schemaVersion`, `eventId`, original `eventType`, `deviceId`, `bootId`, `sequence`, `uptimeMs`, `observedAt`, `clockSource`, `batteryLevelPct` and `errors`. It omits the sensor measurement `payload`; the raw input and its contract remain intact for the separate Telemetry Processor. No event type, timestamp, error or watering decision is inferred.

[RawTelemetryToDeviceDataMapper](src/main/java/com/gladys/deviceprocessor/mapping/RawTelemetryToDeviceDataMapper.java) is a Spring MapStruct mapper:

```java
KafkaDeviceData outbound = mapper.toDeviceData(inbound);
```

MapStruct generates the implementation during compilation. Unmapped source and target fields fail the build; only the intentional input `payload` exclusion is ignored. Error lists and their mutable entries are copied so changing the output cannot modify the input. A null input maps to null. Mapping does not perform runtime Bean Validation or Kafka publication.

The YAML output binding is `spring.cloud.stream.bindings.processDeviceData-out-0`, with destination `${DEVICE_DATA_TOPIC:gladys.device.data.v1}` and JSON content type. Its Kafka producer uses `message-key-expression: headers['kafka_messageKey']` and native String key/value serializers. The function serializes the DTO once and sets the standard `KafkaHeaders.KEY` header from `deviceId`. The Kafka value is plain JSON text, not a JSON-quoted string, and carries no Java type headers. The `processDeviceData` function name associates both input and output bindings. Local auto-startup remains disabled. MapStruct remains a DTO-to-DTO mapper; message wrapping and JSON string serialization stay in the user-authored function.

## Environment and server configuration

| Variable | Purpose/default |
| --- | --- |
| `SPRING_KAFKA_BOOTSTRAPSERVERS` | Kafka addresses; `localhost:9092` on host, `kafka:29092` in Compose |
| `RAW_TELEMETRY_TOPIC` | Input destination; `gladys.telemetry.raw.v1` |
| `DEVICE_DATA_TOPIC` | Output destination; `gladys.device.data.v1` |
| `DEVICE_PROCESSOR_GROUP` | Dedicated group; `gladys.device-data-processor.v1` |
| `SPRING_PROFILES_ACTIVE` | Select `deployed` to activate the function bindings; Compose sets it |
| `SERVER_PORT` | JVM HTTP port; 8081 on host, 8080 in Compose |
| `DEVICE_PROCESSOR_HTTP_PORT` | Compose loopback host port; 8081 |
| `GLADYS_NETWORK` | Existing external Docker network; `gladys` |

Use standard `spring.kafka.*` environment overrides for Kafka security, including `SPRING_KAFKA_SECURITY_PROTOCOL`, `SPRING_KAFKA_PROPERTIES_SASL_MECHANISM` and `SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG`. Binder brokers reference the standard Spring Kafka address rather than maintaining a second address. Topic creation, partition expansion and Kafka admin auto-creation are disabled; provision the topic independently before real consumption is implemented/enabled.

[.env.example](.env.example) contains placeholders only. Spring Boot does not load `.env` automatically. The process must receive environment variables from your shell/process manager/Compose. Reuse the existing shared server file **`/etc/gladys/gladys.env`** outside Git, with mode 600. Compose explicitly forwards only this service's settings; unrelated shared-file values do not enter its container.

Build and start **only this service** from its checkout:

```bash
./mvnw clean verify
docker compose --env-file /etc/gladys/gladys.env up -d --build
```

The Compose stack requires an already-created `gladys` network and independently managed brokers. It neither creates nor starts/stops Kafka or Mosquitto. The Dockerfile uses a digest-pinned Temurin Java 25 JRE, a prebuilt verified JAR and unprivileged UID/GID 10001; the Docker build context excludes source, Git data and env files. The existing error path uses `gladys.device-data-processor.v1.error`; provision this DLQ independently as well. DLQ send failures and delivery guarantees are unchanged by validation: no durable retry/replay or exactly-once guarantee is introduced.

## CI/CD

The workflows under [.github/workflows](.github/workflows/) follow the MQTT bridge's CI/deployment/release pattern, adapted to this service:

- **Device data processor CI** runs on branch pushes, pull requests, manual dispatch and reusable-workflow calls. It sets up Java 25, requires Docker, installs pinned Compose v5.6.0, runs `./mvnw --batch-mode --no-transfer-progress clean verify`, validates the service Compose file and builds `device-data-processor:ci`. Kafka tests cannot silently skip. Reports are retained for 7 days; the verified JAR for 14 days.
- **Deploy device data processor** runs automatically only after a PR merges into `main`, or manually for a selected branch. Unmerged/closed PRs, ordinary branch pushes and tag publication do not deploy. It calls CI first, downloads that commit's `gladys-device-data-processor-<sha>` artifact, builds `device-data-processor:<sha>` and updates only the `device-data-processor` Compose project/service. Deployments across all branches are serialized without cancelling an update in progress.
- **Release device data processor** runs for pushed `v*` tags, calls CI, downloads the exact verified JAR and publishes it to GitHub Releases. Only the publish job gets `contents: write`; it does not deploy containers.

Jobs use the selected Linux **self-hosted** runners. Provide a current runner with Node 24 action support, Bash, Git, Docker CLI/daemon access and outbound access for Java/Maven/action/image downloads. Deployment must execute against the server's Docker daemon. Release publication also requires `gh` on PATH. Java is supplied by setup-java and Maven by the wrapper. Compose setup runs in both build and deployment jobs; installing Docker CLI or mounting its socket alone does not provide the plugin.

Actions retain the bridge's immutable pins: checkout 6.0.3, setup-java 6.2.0, upload-artifact 7.0.2, download-artifact 8.0.2 and Docker setup-compose 2.4.0. The Compose binary is pinned to v5.6.0. Workflows are checked with actionlint; deployment orchestration checks run without Docker using `python3 .github/tests/test_deploy.py` (Python 3 is needed for these local script tests).

The default deployment file is **`/etc/gladys/gladys.env`**; optional GitHub repository/environment variable `GLADYS_ENV_FILE` changes its path. If the file is unreadable inside a socket-mounted runner, the script reads the existing host file through a temporary, digest-pinned Temurin container with a read-only bind, no network and no container logging. Its private temporary copy is removed on exit. A root-owned mode-600 host file can be used without an additional runner bind mount. Missing host files fail deployment; no credentials are fabricated.

The server must already have the external `gladys` network, independent brokers and the raw, device-data and existing DLQ topics. CI provisions topics only inside its test container. Deployment has a 60-second Compose wait for the container to be running; the image has no container healthcheck, so check the server's `/actuator/health` separately. No automatic rollback is configured, and Kafka/Mosquitto are not managed by this workflow.

Merge the workflow into the default branch once to enable manual dispatch. Then select **Actions → Deploy device data processor → Run workflow → Branch**, or run:

```bash
gh workflow run deploy.yml --ref feat/device-data-processor-foundation
```

The selected branch must contain the deployment and reusable CI workflows. Both manual branch deployments and merges target the same server service. Optional protection rules on the `production` GitHub environment must allow branches you intend to deploy manually. Publish a versioned JAR separately with a `v*` tag after setting the desired Maven version.

### Private server env file

A local **`gladys.env`** is prepared for server transfer, ignored by Git and mode 600. It preserves the existing local bridge/shared values and adds this service's settings. The real file is excluded from Docker build contexts and workflow artifacts; only `.env.example` is tracked. Treat the server's existing shared file as authoritative: merge the new entries into `/etc/gladys/gladys.env` and preserve settings added by other services since the local copy was prepared.

The processor-related defaults are:

```dotenv
RAW_TELEMETRY_TOPIC=gladys.telemetry.raw.v1
DEVICE_DATA_TOPIC=gladys.device.data.v1
DEVICE_PROCESSOR_GROUP=gladys.device-data-processor.v1
DEVICE_PROCESSOR_HTTP_PORT=8081
```

Keep shared `SPRING_KAFKA_BOOTSTRAPSERVERS=kafka:29092` and `GLADYS_NETWORK=gladys` when using the supplied container network, or retain your own working values. Do not put a generic `SPRING_PROFILES_ACTIVE=deployed` into the shared file for every application; this service's Compose explicitly selects its profile. For manual maintenance, use the same shared file:

```bash
docker compose --env-file /etc/gladys/gladys.env logs -f device-data-processor
```

GitHub's [merged PR events](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#running-your-pull_request-workflow-when-a-pull-request-merges) and [manual branch selection](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow) describe the trigger behavior.

## Versions and official compatibility references

| Component | Version |
| --- | --- |
| Java | 25, no preview |
| Spring Boot / Boot Maven plugin | 4.1.2 |
| Spring Cloud BOM | 2025.1.4 |
| Cloud Stream / Function | 5.0.4 / 5.0.5, Cloud managed |
| Spring Kafka / Kafka clients | 4.1.2 / 4.2.2, Boot managed |
| Reactor | 3.8.8, Boot managed |
| Lombok | 1.18.48, Boot managed |
| JUnit Jupiter / Testcontainers | 6.0.3 / 2.0.5, Boot managed |
| OpenAPI Generator | 7.26.0 |
| MapStruct / Lombok binding | 1.6.3 / 0.2.0 |
| Swagger parser / JSON Schema validator | 2.1.48 / 3.0.9, test-only |
| Maven / Wrapper | 3.9.12 / 3.3.4, Maven distribution SHA-256 pinned |
| Compiler / Surefire / Failsafe | 3.15.0 / 3.5.6 / 3.5.6, parent managed |
| Kafka test image | `apache/kafka:4.1.0` |

SLF4J and shared dependencies remain Boot managed. Generator output targets Spring Boot 4/Jackson 3. Models are generated at build time and used at runtime.

- [Spring Boot system requirements](https://docs.spring.io/spring-boot/system-requirements.html): 4.1.2 supports Java 25 and Maven 3.6.3+.
- [Spring Cloud supported versions](https://github.com/spring-cloud/spring-cloud-release/wiki/Supported-Versions), [functional bindings](https://docs.spring.io/spring-cloud-stream/reference/spring-cloud-stream/producing-and-consuming-messages.html), [Kafka binder configuration](https://docs.spring.io/spring-cloud-stream/reference/kafka/kafka-binder/config-options.html).
- [OpenAPI Spring generator](https://openapi-generator.tech/docs/generators/spring/), [Maven plugin](https://openapi-generator.tech/docs/plugins/).
- [MapStruct setup and strict mappings](https://mapstruct.org/documentation/stable/reference/html/), [Lombok integration](https://mapstruct.org/documentation/stable/reference/html/#lombok).
- [Lombok Java support](https://projectlombok.org/changelog), [Testcontainers Kafka](https://java.testcontainers.org/modules/kafka/).
- [Maven Wrapper](https://maven.apache.org/tools/wrapper/), [Surefire](https://maven.apache.org/surefire/maven-surefire-plugin/), [Failsafe](https://maven.apache.org/surefire/maven-failsafe-plugin/).

The output uses [Kafka binder producer configuration](https://docs.spring.io/spring-cloud-stream/reference/kafka/kafka-binder/config-options.html) and Spring Kafka's [serialization support](https://docs.spring.io/spring-kafka/reference/kafka/serdes.html).

Sensor payload update: `sensorId` is a stable UUID. OpenAPI generates its Java property as `UUID`, and generates typed `Long` ADC values (`rawMoisture`, `rawMoistureMin`, `rawMoistureMax`) and positive `Integer sampleCount`. `rawMoisture` is the rounded sample mean. `calibrationId` remains an optional nonblank string. Moisture messages omit `unit`, ADC resolution and the previous fixed quality label; other sensor types can still use extension properties. Calibration settings belong to the sensor/calibration record. The bridge preserves all these fields; the device processor intentionally excludes the entire sensor payload from device-data output.

Regenerate DTOs and MapStruct implementations with the normal Maven build after this contract change; do not edit generated files under `target/`. Examples and mapping/validation test sources were updated, but tests and builds were not run at the user's request. Legacy non-UUID sensor IDs must be replaced when updating firmware and consumers.

Power telemetry may include measured `supplyVoltageMv` (board rail millivolts). The optional `batteryLevelPct` field is omitted when there is no battery charge reading. Older messages with a reported battery percentage remain valid. Deploy updated consumers before enabling this firmware format.

## Diagnostic condition codes

New observations use condition codes with three uppercase alphanumeric characters, an underscore and two uppercase alphanumeric characters, for example `BAT_01`. The OpenAPI `DeviceError.code` definition validates this six-character format. The catalog is extensible through additional well-formed identifiers.

| Code | Condition |
| --- | --- |
| `BAT_01` | Low battery charge |
| `BAT_02` | Battery measurement unavailable |
| `SEN_01` | Sensor read failed |
| `SEN_02` | Sensor reading outside its supported range |
| `PMP_01` | Pump activation failed |
| `PMP_02` | Pump failed to stop |

Keep a condition's code stable as its severity changes. Within a device entry, `(component, code)` identifies a condition. Use a stable component identifier for each physical instance, such as `SOIL_SENSOR_A` and `SOIL_SENSOR_B`.

The `errors` array declares the complete active diagnostic snapshot. For two ordered, complete observations, compare their sets of `(component, code)` pairs. A pair absent from the next snapshot has resolved. For example, critical `SEN_01` and critical `PMP_01` are two conditions; a later snapshot containing only `PMP_01` indicates that the sensor condition has resolved. Reconciliation requires complete snapshots and a reliable observation order. Diagnostic reconciliation and history transitions belong to the device service.

Coordinate producer and consumer updates when adopting the compact format. Incoming legacy descriptive codes such as `LOW_BATTERY` fail the new validation. Existing persisted device summaries retain their original codes for historical compatibility.
