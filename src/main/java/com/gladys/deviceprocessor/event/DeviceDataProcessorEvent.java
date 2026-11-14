package com.gladys.deviceprocessor.event;

import com.gladys.deviceprocessor.mapping.RawTelemetryToDeviceDataMapper;
import com.gladys.deviceprocessor.message.KafkaDeviceData;
import com.gladys.deviceprocessor.message.KafkaRawTelemetry;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.util.function.Function;

@Slf4j
@Service
@AllArgsConstructor
public class DeviceDataProcessorEvent {

    private final ObjectMapper objectMapper;
    private final RawTelemetryToDeviceDataMapper rawTelemetryToDeviceDataMapper;
    private final StreamBridge streamBridge;
    private final Validator validator;

    private static final String DLQ_TOPIC = "gladys.device-data-processor.v1.error";

    @Bean
    public Function<Flux<Message<String>>, Flux<Message<String>>> processDeviceData() {
        return data -> data
                .flatMap(msg -> processSingleDevice(msg)
                        .onErrorResume(err -> {
                            sendToDlq(msg, err);
                            return Mono.empty();
                        })
                );
    }

    private Mono<Message<String>> processSingleDevice(Message<String> rawTelemetry) {
        return Mono.fromCallable(() -> {
            var deviceData = extractRawTelemetry(rawTelemetry);
            if (deviceData == null) {
                throw new RuntimeException(rawTelemetry.getPayload());
            }
            var violations = validator.validate(deviceData);
            if (!violations.isEmpty()) {
                throw new ConstraintViolationException(violations);
            }
            KafkaDeviceData device = rawTelemetryToDeviceDataMapper.toDeviceData(deviceData);
            return MessageBuilder.withPayload(objectMapper.writeValueAsString(device))
                    .copyHeaders(rawTelemetry.getHeaders())
                    .setHeader(KafkaHeaders.KEY, device.getDeviceId().toString())
                    .build();
        });
    }

    private void sendToDlq(Message<String> message, Throwable error) {
        Message<String> dlqMessage = MessageBuilder
                .withPayload(message.getPayload())
                .copyHeaders(message.getHeaders())
                .setHeader("x-exception-message", error.getMessage())
                .build();

        // Sends to the binding named 'dlq-out-0'
        streamBridge.send(DLQ_TOPIC, dlqMessage);
    }

    private KafkaRawTelemetry extractRawTelemetry(Message<String> message) {
        try {
            return objectMapper.readValue(message.getPayload(), KafkaRawTelemetry.class);
        } catch (Exception e) {
            log.error("Error while parsing raw telemetry json", e);
            return null;
        }
    }
}
