package com.gladys.deviceprocessor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaLocation;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import io.swagger.v3.parser.OpenAPIV3Parser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.yaml.snakeyaml.Yaml;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.assertThat;

class RawTelemetryContractTest {
    private static final Path CONTRACT = Path.of("src/main/resources/openapi/kafka-raw-telemetry.yml");
    private final JsonMapper json = JsonMapper.builder().build();

    @Test void declaresOneStandaloneOpenApiMessageContract() {
        var parsed = new OpenAPIV3Parser().readLocation(CONTRACT.toAbsolutePath().toString(), null, null);
        assertThat(parsed.getMessages()).isEmpty();
        assertThat(parsed.getOpenAPI().getOpenapi()).isEqualTo("3.1.2");
        assertThat(parsed.getOpenAPI().getPaths()).isEmpty();
        assertThat(parsed.getOpenAPI().getComponents().getSchemas())
            .containsOnlyKeys("KafkaRawTelemetry", "TelemetryPayload", "DeviceError");
    }

    @Test void acceptsTheFullBridgeRawRecord() throws Exception {
        assertThat(validate(example())).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"schemaVersion", "eventId", "eventType", "deviceId", "bootId", "sequence",
        "uptimeMs", "observedAt", "clockSource", "mock", "batteryLevelPct", "errors", "payload"})
    void requiresEachEnvelopeField(String field) throws Exception {
        var value = example();
        value.remove(field);
        assertThat(validate(value)).isNotEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"deviceId", "eventId", "bootId"})
    void rejectsInvalidIdentifiers(String field) throws Exception {
        var value = example();
        value.put(field, "not-a-uuid");
        assertThat(validate(value)).isNotEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"sequence", "uptimeMs", "observedAt", "batteryLevelPct"})
    void rejectsNegativeValues(String field) throws Exception {
        var value = example();
        value.put(field, -1);
        assertThat(validate(value)).isNotEmpty();
    }

    @Test void rejectsUnsupportedVersionsAndOutOfRangeBattery() throws Exception {
        var value = example();
        value.put("schemaVersion", 2);
        assertThat(validate(value)).isNotEmpty();
        value = example();
        value.put("batteryLevelPct", 101);
        assertThat(validate(value)).isNotEmpty();
    }

    @Test void requiresSensorIdentityAndKnownErrorSeverity() throws Exception {
        var value = example();
        ((ObjectNode) value.get("payload")).remove("sensorId");
        assertThat(validate(value)).isNotEmpty();
        value = example();
        ((ObjectNode) value.get("errors").get(0)).put("severity", "UNKNOWN");
        assertThat(validate(value)).isNotEmpty();
    }

    @Test void acceptsDifferentSensorsWithoutRequiringSoilOrPlantIds() throws Exception {
        var value = example();
        value.put("eventType", "SOIL_TEMPERATURE_READING");
        value.putObject("payload").put("sensorId", "5556111b-70f6-4185-aa89-f66503bc7423").put("value", 18.5).put("unit", "degC");
        assertThat(validate(value)).isEmpty();
    }

    @Test void rejectsInvalidSensorUuidsAndSamplingValues() throws Exception {
        var value = example();
        ((ObjectNode) value.get("payload")).put("sensorId", "soil-001");
        assertThat(validate(value)).isNotEmpty();
        for (String field : java.util.List.of("rawMoisture", "rawMoistureMin", "rawMoistureMax")) {
            value = example();
            ((ObjectNode) value.get("payload")).put(field, -1L);
            assertThat(validate(value)).isNotEmpty();
            ((ObjectNode) value.get("payload")).put(field, 4_294_967_296L);
            assertThat(validate(value)).isNotEmpty();
        }
        value = example();
        ((ObjectNode) value.get("payload")).put("sampleCount", 0);
        assertThat(validate(value)).isNotEmpty();
    }

    private ObjectNode example() throws Exception {
        return (ObjectNode) json.readTree(Files.readString(CONTRACT.resolveSibling("examples/kafka-raw-telemetry.json")));
    }

    private java.util.List<com.networknt.schema.Error> validate(ObjectNode value) throws Exception {
        String location = "https://schemas.gladys.test/kafka-raw-telemetry.yml";
        String document = json.writeValueAsString(new Yaml().load(Files.readString(CONTRACT)));
        Schema schema = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12,
            builder -> builder.schemas(Map.of(location, document)))
            .getSchema(SchemaLocation.of(location + "#/components/schemas/KafkaRawTelemetry"));
        return schema.validate(value.toString(), InputFormat.JSON,
            context -> context.executionConfig(config -> config.formatAssertionsEnabled(true)));
    }
}
