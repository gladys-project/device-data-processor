package com.gladys.deviceprocessor.mapping;

import java.nio.file.Files;
import java.nio.file.Path;
import com.gladys.deviceprocessor.message.KafkaRawTelemetry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = "spring.config.location=classpath:/application-test.yml")
class RawTelemetryToDeviceDataMapperTest {
    private final RawTelemetryToDeviceDataMapper mapper;
    private final JsonMapper json;

    @Autowired RawTelemetryToDeviceDataMapperTest(RawTelemetryToDeviceDataMapper mapper, JsonMapper json) {
        this.mapper = mapper;
        this.json = json;
    }

    private KafkaRawTelemetry input() throws Exception {
        return json.readValue(Files.readString(Path.of("src/main/resources/openapi/examples/kafka-raw-telemetry.json")),
            KafkaRawTelemetry.class);
    }

    @Test void projectsDeviceDataAndCorrelationWithoutSensorMeasurements() throws Exception {
        var source = input();
        var actual = mapper.toDeviceData(source);
        ObjectNode expected = (ObjectNode) json.readTree(json.writeValueAsString(source));
        expected.remove("payload");
        assertThat(json.readTree(json.writeValueAsString(actual))).isEqualTo(expected);
        assertThat(actual.getDeviceId()).isEqualTo(source.getDeviceId());
        assertThat(actual.getEventId()).isEqualTo(source.getEventId());
        assertThat(actual.getBootId()).isEqualTo(source.getBootId());
        assertThat(actual.getSequence()).isEqualTo(source.getSequence());
        assertThat(actual.getBatteryLevelPct()).isEqualTo(10);
        assertThat(actual.getErrors().getFirst().getCode()).isEqualTo("LOW_BATTERY");
        assertThat(source.getPayload().getSensorId()).isEqualTo(java.util.UUID.fromString("e62e100e-4e97-42e0-8dd8-46519f0114c9"));
    }

    @Test void copiesDiagnosticCollectionsAndElementsRatherThanSharingMutableState() throws Exception {
        var source = input();
        var actual = mapper.toDeviceData(source);
        assertThat(actual.getErrors()).isNotSameAs(source.getErrors());
        assertThat(actual.getErrors().getFirst()).isNotSameAs(source.getErrors().getFirst());
        actual.getErrors().getFirst().setCode("CHANGED");
        actual.getErrors().clear();
        assertThat(source.getErrors()).hasSize(1);
        assertThat(source.getErrors().getFirst().getCode()).isEqualTo("LOW_BATTERY");
    }

    @Test void preservesEmptyDiagnosticsAndRealDeviceFlag() throws Exception {
        var source = input();
        source.getErrors().clear();
        source.setMock(false);
        source.setBatteryLevelPct(80);
        var actual = mapper.toDeviceData(source);
        assertThat(actual.getErrors()).isEmpty();
        assertThat(actual.getMock()).isFalse();
        assertThat(actual.getBatteryLevelPct()).isEqualTo(80);
    }

    @Test void projectsSupplyVoltageWithoutAddingAnUnknownBatteryPercentage() throws Exception {
        var source = input();
        source.getErrors().clear();
        source.setBatteryLevelPct(null);
        source.setSupplyVoltageMv(4750);

        var output = json.readTree(json.writeValueAsString(mapper.toDeviceData(source)));

        assertThat(output.get("supplyVoltageMv").asInt()).isEqualTo(4750);
        assertThat(output.has("batteryLevelPct")).isFalse();
        assertThat(output.get("errors").size()).isZero();
    }

    @Test void projectsDifferentSensorsWithoutInterpretingTheirMeasurements() throws Exception {
        var source = input();
        source.setEventType("SOIL_TEMPERATURE_READING");
        source.getPayload().setSensorId(java.util.UUID.fromString("5556111b-70f6-4185-aa89-f66503bc7423"));
        source.getPayload().putAdditionalProperty("value", 18.5);
        var actual = mapper.toDeviceData(source);
        assertThat(actual.getEventType()).isEqualTo("SOIL_TEMPERATURE_READING");
        assertThat(actual.getDeviceId()).isEqualTo(source.getDeviceId());
        assertThat(json.readTree(json.writeValueAsString(actual)).has("payload")).isFalse();
        assertThat(source.getPayload().getAdditionalProperties()).containsEntry("value", 18.5);
    }

    @Test void nullSourceMapsToNull() {
        assertThat(mapper.toDeviceData(null)).isNull();
    }
}
