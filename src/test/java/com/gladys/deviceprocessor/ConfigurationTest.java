package com.gladys.deviceprocessor;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.function.context.FunctionProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import static org.assertj.core.api.Assertions.assertThat;

class ConfigurationTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withUserConfiguration(BoundConfiguration.class)
        .withInitializer(new ConfigDataApplicationContextInitializer())
        .withPropertyValues("spring.config.location=classpath:/application.yml");

    @Test void declaresRawInputButLeavesLocalConsumptionInactive() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            var bindings = context.getBean(BindingServiceProperties.class);
            var input = bindings.getBindingProperties("processDeviceData-in-0");
            assertThat(input.getDestination()).isEqualTo("gladys.telemetry.raw.v1");
            assertThat(input.getGroup()).isEqualTo("gladys.device-data-processor.v1");
            assertThat(input.getContentType()).isEqualTo("application/json");
            assertThat(bindings.getConsumerProperties("processDeviceData-in-0").isAutoStartup()).isFalse();
            assertThat(context.getBean(FunctionProperties.class).getDefinition()).isEmpty();
            assertThat(context.getEnvironment().getProperty("spring.cloud.stream.function.autodetect", Boolean.class)).isFalse();
            assertThat(context.getBean(KafkaProperties.class).getAdmin().isAutoCreate()).isFalse();
        });
    }

    @Test void declaresDeviceDataOutputWithoutStartingItsProducerLocally() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            var bindings = context.getBean(BindingServiceProperties.class);
            var output = bindings.getBindingProperties("processDeviceData-out-0");
            assertThat(output.getDestination()).isEqualTo("gladys.device.data.v1");
            assertThat(output.getContentType()).isEqualTo("application/json");
            assertThat(bindings.getProducerProperties("processDeviceData-out-0").isAutoStartup()).isFalse();
            assertThat(bindings.getProducerProperties("processDeviceData-out-0").isUseNativeEncoding()).isTrue();
            assertThat(context.getEnvironment().getProperty(
                "spring.cloud.stream.kafka.bindings.processDeviceData-out-0.producer.message-key-expression"))
                .isEqualTo("headers['kafka_messageKey']");
        });
    }

    @Test void deployedProfileDeclaresFutureFunctionAndActivatesItsInputBinding() {
        runner.withPropertyValues("spring.profiles.active=deployed").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(FunctionProperties.class).getDefinition()).isEqualTo("processDeviceData");
            assertThat(context.getBean(BindingServiceProperties.class)
                .getConsumerProperties("processDeviceData-in-0").isAutoStartup()).isTrue();
            assertThat(context.getEnvironment().getProperty("spring.cloud.stream.kafka.binder.auto-create-topics", Boolean.class)).isFalse();
            assertThat(context.getBean(BindingServiceProperties.class)
                .getProducerProperties("processDeviceData-out-0").isAutoStartup()).isTrue();
        });
    }

    @Test void environmentStyleOverridesReachSpringKafkaAndSpringCloudBindings() {
        runner.withInitializer(context -> context.getEnvironment().getPropertySources().addFirst(
            new SystemEnvironmentPropertySource("systemEnvironment", Map.of(
                "SPRING_KAFKA_BOOTSTRAPSERVERS", "broker.test:29092",
                "RAW_TELEMETRY_TOPIC", "gladys.telemetry.test.v1",
                "DEVICE_DATA_TOPIC", "gladys.device.data.test.v1",
                "DEVICE_PROCESSOR_GROUP", "device-processor-test"))))
            .run(context -> {
                assertThat(context).hasNotFailed();
                var input = context.getBean(BindingServiceProperties.class).getBindingProperties("processDeviceData-in-0");
                assertThat(input.getDestination()).isEqualTo("gladys.telemetry.test.v1");
                assertThat(input.getGroup()).isEqualTo("device-processor-test");
                assertThat(context.getBean(BindingServiceProperties.class)
                    .getBindingProperties("processDeviceData-out-0").getDestination())
                    .isEqualTo("gladys.device.data.test.v1");
                assertThat(context.getBean(KafkaProperties.class).getBootstrapServers()).containsExactly("broker.test:29092");
                assertThat(context.getEnvironment().getProperty("spring.cloud.stream.kafka.binder.brokers")).isEqualTo("broker.test:29092");
            });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableIntegration
    @EnableConfigurationProperties({BindingServiceProperties.class, FunctionProperties.class, KafkaProperties.class})
    static class BoundConfiguration { }
}
