package com.gladys.deviceprocessor;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.assertThat;

/** Exercises the real production string function; no test replacement function. */
@Testcontainers
@ActiveProfiles("deployed")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = "spring.config.location=classpath:/application.yml,classpath:/application-test.yml")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class KafkaBindingIT {
    private static final String INPUT = "gladys.telemetry.raw.v1";
    private static final String OUTPUT = "gladys.device.data.v1";
    private static final String DLQ = "gladys.device-data-processor.v1.error";
    private static final String DEVICE_ID = "550e8400-e29b-41d4-a716-446655440000";
    private static final String GROUP = "device-processor-it-" + UUID.randomUUID();
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0")
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .waitingFor(Wait.forLogMessage(".*Kafka Server started.*\\n", 1))
        .withStartupTimeout(Duration.ofSeconds(90));

    @DynamicPropertySource
    static void broker(DynamicPropertyRegistry properties) throws Exception {
        var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "request.timeout.ms", 10000, "default.api.timeout.ms", 15000));
        try {
            admin.createTopics(List.of(new NewTopic(INPUT, 1, (short) 1),
                new NewTopic(OUTPUT, 1, (short) 1), new NewTopic(DLQ, 1, (short) 1)))
                .all().get(15, TimeUnit.SECONDS);
        } finally {
            admin.close(Duration.ofSeconds(3));
        }
        properties.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        properties.add("spring.cloud.stream.kafka.binder.brokers", KAFKA::getBootstrapServers);
        properties.add("spring.cloud.stream.bindings.processDeviceData-in-0.group", () -> GROUP);
    }

    private final KafkaTemplate<String, byte[]> kafka;
    private final ConsumerFactory<String, byte[]> consumers;
    private final ObjectMapper json;

    @Autowired KafkaBindingIT(KafkaTemplate<String, byte[]> kafka,
                              ConsumerFactory<String, byte[]> consumers, ObjectMapper json) {
        this.kafka = kafka;
        this.consumers = consumers;
        this.json = json;
    }

    private ObjectNode example() throws Exception {
        var body = (ObjectNode) json.readTree(Files.readString(Path.of("src/main/resources/openapi/examples/kafka-raw-telemetry.json")));
        body.put("eventId", UUID.randomUUID().toString());
        return body;
    }

    private ConsumerRecord<String, byte[]> publishAndReceive(ObjectNode input, String destination) throws Exception {
        var consumer = consumers.createConsumer("observer-" + UUID.randomUUID(), "observer", "-1");
        try {
            var partition = new TopicPartition(destination, 0);
            consumer.assign(List.of(partition));
            consumer.seekToEnd(List.of(partition));
            consumer.position(partition, Duration.ofSeconds(5));
            kafka.send(INPUT, DEVICE_ID, json.writeValueAsBytes(input)).get(15, TimeUnit.SECONDS);
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(250))) {
                    var body = json.readTree(record.value());
                    if (body.isObject() && input.get("eventId").asString().equals(body.get("eventId").asString())) {
                        return record;
                    }
                }
            }
            throw new AssertionError("Event did not arrive on " + destination);
        } finally {
            consumer.close(Duration.ofSeconds(3));
        }
    }

    @Test
    @Timeout(45)
    void actualStringFunctionPublishesPlainDeviceJsonWithDeviceKey() throws Exception {
        ObjectNode input = example();
        var record = publishAndReceive(input, OUTPUT);
        ObjectNode expected = input.deepCopy();
        expected.remove("payload");
        assertThat(record.key()).isEqualTo(DEVICE_ID);
        assertThat(json.readTree(record.value())).isEqualTo(expected);
        assertThat(record.headers().lastHeader("__TypeId__")).isNull();
    }

    @Test
    @Timeout(45)
    void generatedMessageConstraintsRouteInvalidInputToExistingDlq() throws Exception {
        ObjectNode invalid = example();
        invalid.put("batteryLevelPct", 101);
        var record = publishAndReceive(invalid, DLQ);
        assertThat(json.readTree(record.value())).isEqualTo(invalid);
        var error = record.headers().lastHeader("x-exception-message");
        assertThat(error).isNotNull();
        assertThat(new String(error.value(), StandardCharsets.UTF_8)).contains("batteryLevelPct");
    }

    @Test
    @Timeout(45)
    void anotherSensorTypeStillUsesSameDeviceKeyAndPreservesObservationMetadata() throws Exception {
        ObjectNode input = example();
        input.put("eventType", "SOIL_TEMPERATURE_READING");
        input.putObject("payload").put("sensorId", "temperature-002").put("value", 18.5).put("unit", "degC");
        var record = publishAndReceive(input, OUTPUT);
        ObjectNode expected = input.deepCopy();
        expected.remove("payload");
        assertThat(record.key()).isEqualTo(DEVICE_ID);
        assertThat(json.readTree(record.value())).isEqualTo(expected);
    }
}
