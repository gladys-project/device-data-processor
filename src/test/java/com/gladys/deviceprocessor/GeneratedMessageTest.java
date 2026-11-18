package com.gladys.deviceprocessor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import com.gladys.deviceprocessor.message.KafkaRawTelemetry;
import com.gladys.deviceprocessor.message.KafkaDeviceData;
import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.assertThat;

class GeneratedMessageTest {
    private final JsonMapper json = JsonMapper.builder().build();
    private ObjectNode example() throws Exception {
        return (ObjectNode) json.readTree(Files.readString(Path.of("src/main/resources/openapi/examples/kafka-raw-telemetry.json")));
    }
    @Test void bindsFullRawRecordWithoutLosingDeviceOrSensorData() throws Exception {
        var original = example();
        var message = json.treeToValue(original, KafkaRawTelemetry.class);
        assertThat(message.getDeviceId()).isEqualTo(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        assertThat(message.getObservedAt()).isEqualTo(1794164428500L);
        assertThat(message.getBatteryLevelPct()).isEqualTo(10);
        assertThat(message.getErrors().getFirst().getCode()).isEqualTo("LOW_BATTERY");
        assertThat(message.getPayload().getSensorId()).isEqualTo(java.util.UUID.fromString("e62e100e-4e97-42e0-8dd8-46519f0114c9"));
        assertThat(message.getPayload().getRawMoisture()).isEqualTo(2550L);
        assertThat(message.getPayload().getRawMoistureMin()).isEqualTo(2542L);
        assertThat(message.getPayload().getRawMoistureMax()).isEqualTo(2560L);
        assertThat(message.getPayload().getSampleCount()).isEqualTo(16);
        assertThat(json.readTree(json.writeValueAsString(message))).isEqualTo(original);
    }
    @Test void acceptsDifferentSensorMeasurementsOnTheSameDevice() throws Exception {
        var original = example();
        original.put("eventType", "SOIL_TEMPERATURE_READING");
        original.putObject("payload").put("sensorId", "5556111b-70f6-4185-aa89-f66503bc7423").put("value", 18.5).put("unit", "degC");
        var message = json.treeToValue(original, KafkaRawTelemetry.class);
        assertThat(json.readTree(json.writeValueAsString(message))).isEqualTo(original);
    }
    @Test void generatedBeanValidationCascadesIntoDeviceDiagnosticsAndSensor() throws Exception {
        var message = json.treeToValue(example(), KafkaRawTelemetry.class);
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            var validator = factory.getValidator();
            assertThat(validator.validate(message)).isEmpty();
            message.setBatteryLevelPct(101);
            message.getPayload().setSensorId(null);
            message.getPayload().setSampleCount(0);
            message.getErrors().getFirst().setCode("");
            assertThat(validator.validate(message)).extracting(v -> v.getPropertyPath().toString())
                .contains("batteryLevelPct", "payload.sensorId", "payload.sampleCount", "errors[0].code");
        }
    }
    @Test void outboundModelAlsoEnforcesDefinitionConstraints() throws Exception {
        var message = json.readValue(Files.readString(Path.of("src/main/resources/openapi/examples/kafka-device-data.json")),
            KafkaDeviceData.class);
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            var validator = factory.getValidator();
            assertThat(validator.validate(message)).isEmpty();
            message.setDeviceId(null);
            message.setBatteryLevelPct(101);
            message.getErrors().add(null);
            assertThat(validator.validate(message)).extracting(v -> v.getPropertyPath().toString())
                .contains("deviceId", "batteryLevelPct", "errors[1].<list element>");
        }
    }

}
