package com.gladys.deviceprocessor.event;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.function.Function;
import com.gladys.deviceprocessor.mapping.RawTelemetryToDeviceDataMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = "spring.config.location=classpath:/application-test.yml")
class DeviceDataProcessorEventTest {
    @Autowired private DeviceDataProcessorEvent processor;
    @Autowired private JsonMapper json;
    @MockitoBean private StreamBridge bridge;
    @MockitoSpyBean private RawTelemetryToDeviceDataMapper mapper;

    @BeforeEach void allowDlqSend() {
        when(bridge.send(anyString(), any())).thenReturn(true);
    }

    private ObjectNode example() throws Exception {
        return (ObjectNode) json.readTree(Files.readString(Path.of("src/main/resources/openapi/examples/kafka-raw-telemetry.json")));
    }

    @Test void preservesStringFunctionContractAndProducesPlainJsonWithDeviceKey() throws Exception {
        Function<Flux<Message<String>>, Flux<Message<String>>> function = processor.processDeviceData();
        ObjectNode original = example();
        ObjectNode expected = original.deepCopy();
        expected.remove("payload");
        Message<String> input = MessageBuilder.withPayload(json.writeValueAsString(original))
            .setHeader("test-correlation", "trace-123").build();
        StepVerifier.create(function.apply(Flux.just(input)))
            .assertNext(output -> {
                assertThat(json.readTree(output.getPayload())).isEqualTo(expected);
                assertThat(output.getHeaders().get(KafkaHeaders.KEY)).isEqualTo(original.get("deviceId").asString());
                assertThat(output.getHeaders().get("test-correlation")).isEqualTo("trace-123");
            }).expectComplete().verify(Duration.ofSeconds(3));
    }
    @ParameterizedTest
    @ValueSource(strings = {"battery-under", "battery-over", "missing-sensor", "null-deviceId",
        "negative-sequence", "negative-uptimeMs", "negative-observedAt", "blank-eventType",
        "blank-clockSource", "error-code", "error-message", "missing-errors", "null-error-entry"})
    void constraintsFromGeneratedMessageRejectInvalidRecordsBeforeMapping(String kind) throws Exception {
        ObjectNode body = example();
        String path;
        switch (kind) {
            case "battery-under" -> { body.put("batteryLevelPct", -1); path = "batteryLevelPct"; }
            case "battery-over" -> { body.put("batteryLevelPct", 101); path = "batteryLevelPct"; }
            case "missing-sensor" -> { ((ObjectNode) body.get("payload")).remove("sensorId"); path = "payload.sensorId"; }
            case "null-deviceId" -> { body.putNull("deviceId"); path = "deviceId"; }
            case "negative-sequence" -> { body.put("sequence", -1); path = "sequence"; }
            case "negative-uptimeMs" -> { body.put("uptimeMs", -1); path = "uptimeMs"; }
            case "negative-observedAt" -> { body.put("observedAt", -1); path = "observedAt"; }
            case "blank-eventType" -> { body.put("eventType", " "); path = "eventType"; }
            case "blank-clockSource" -> { body.put("clockSource", " "); path = "clockSource"; }
            case "error-code" -> { ((ObjectNode) body.get("errors").get(0)).put("code", ""); path = "errors[0].code"; }
            case "error-message" -> { ((ObjectNode) body.get("errors").get(0)).put("message", " "); path = "errors[0].message"; }
            case "missing-errors" -> { body.remove("errors"); path = "errors"; }
            case "null-error-entry" -> { body.putArray("errors").addNull(); path = "errors[0]"; }
            default -> throw new AssertionError(kind);
        }
        String original = json.writeValueAsString(body);
        Message<String> input = MessageBuilder.withPayload(original).build();
        StepVerifier.create(processor.processDeviceData().apply(Flux.just(input)))
            .expectComplete().verify(Duration.ofSeconds(3));
        verify(mapper, never()).toDeviceData(any());
        ArgumentCaptor<Message<?>> capture = ArgumentCaptor.forClass(Message.class);
        verify(bridge).send(eq("gladys.device-data-processor.v1.error"), capture.capture());
        assertThat(capture.getValue().getPayload()).isEqualTo(original);
        assertThat(capture.getValue().getHeaders().get("x-exception-message", String.class)).contains(path);
    }

}
