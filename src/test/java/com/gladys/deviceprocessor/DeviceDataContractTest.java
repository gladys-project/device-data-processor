package com.gladys.deviceprocessor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaLocation;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.swagger.v3.parser.core.models.ParseOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.yaml.snakeyaml.Yaml;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.assertThat;

class DeviceDataContractTest {
    private static final Path DIRECTORY = Path.of("src/main/resources/openapi");
    private final JsonMapper json = JsonMapper.builder().build();

    @Test void outboundIsASeparateOpenApiContractWithSharedDiagnostics() {
        var options = new ParseOptions();
        options.setResolve(true);
        var parsed = new OpenAPIV3Parser().readLocation(DIRECTORY.resolve("kafka-device-data.yml").toAbsolutePath().toString(), null, options);
        assertThat(parsed.getMessages()).isEmpty();
        assertThat(parsed.getOpenAPI().getPaths()).isEmpty();
        assertThat(parsed.getOpenAPI().getComponents().getSchemas()).containsKeys("KafkaDeviceData", "DeviceError");
    }

    @Test void outboundExampleContainsDeviceDiagnosticsAndOriginalCorrelation() throws Exception {
        var value = example();
        assertThat(validate(value)).isEmpty();
        ObjectNode raw = (ObjectNode) json.readTree(Files.readString(DIRECTORY.resolve("examples/kafka-raw-telemetry.json")));
        raw.remove("payload");
        assertThat(value).isEqualTo(raw);
    }

    @ParameterizedTest
    @ValueSource(strings = {"schemaVersion", "eventId", "eventType", "deviceId", "bootId", "sequence",
        "uptimeMs", "observedAt", "clockSource", "errors"})
    void requiresDeviceDiagnosticsAndCorrelationFields(String field) throws Exception {
        var value = example();
        value.remove(field);
        assertThat(validate(value)).isNotEmpty();
    }

    @Test void rejectsSensorPayloadInDeviceDataOutput() throws Exception {
        var value = example();
        value.putObject("payload").put("sensorId", "e62e100e-4e97-42e0-8dd8-46519f0114c9").put("rawMoisture", 2550);
        assertThat(validate(value)).isNotEmpty();
    }

    @Test void rejectsInvalidDeviceIdentityAndDiagnostics() throws Exception {
        var value = example();
        value.put("deviceId", "not-a-uuid");
        assertThat(validate(value)).isNotEmpty();
        value = example();
        value.put("schemaVersion", 2);
        assertThat(validate(value)).isNotEmpty();
        value = example();
        value.put("batteryLevelPct", 101);
        assertThat(validate(value)).isNotEmpty();
        value = example();
        ((ObjectNode) value.get("errors").get(0)).put("severity", "UNKNOWN");
        assertThat(validate(value)).isNotEmpty();
    }

    private ObjectNode example() throws Exception {
        return (ObjectNode) json.readTree(Files.readString(DIRECTORY.resolve("examples/kafka-device-data.json")));
    }

    private java.util.List<com.networknt.schema.Error> validate(ObjectNode value) throws Exception {
        String base = "https://schemas.gladys.test/";
        var documents = new HashMap<String, String>();
        for (String name : java.util.List.of("kafka-raw-telemetry.yml", "kafka-device-data.yml")) {
            documents.put(base + name, json.writeValueAsString(new Yaml().load(Files.readString(DIRECTORY.resolve(name)))));
        }
        Schema schema = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12,
            builder -> builder.schemas(documents)).getSchema(SchemaLocation.of(base + "kafka-device-data.yml#/components/schemas/KafkaDeviceData"));
        return schema.validate(value.toString(), InputFormat.JSON,
            context -> context.executionConfig(config -> config.formatAssertionsEnabled(true)));
    }
}
