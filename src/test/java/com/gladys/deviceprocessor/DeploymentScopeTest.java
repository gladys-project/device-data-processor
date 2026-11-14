package com.gladys.deviceprocessor;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = "spring.config.location=classpath:/application.yml,classpath:/application-test.yml")
class DeploymentScopeTest {
    private final ApplicationContext context;
    private final BindingServiceProperties bindings;
    private final KafkaListenerEndpointRegistry listeners;

    @Autowired DeploymentScopeTest(ApplicationContext context, BindingServiceProperties bindings,
                                  KafkaListenerEndpointRegistry listeners) {
        this.context = context;
        this.bindings = bindings;
        this.listeners = listeners;
    }

    @Test void functionExistsButLocalBindingsStayInactive() {
        assertThat(context.containsBean("processDeviceData")).isTrue();
        assertThat(bindings.getConsumerProperties("processDeviceData-in-0").isAutoStartup()).isFalse();
        assertThat(bindings.getProducerProperties("processDeviceData-out-0").isAutoStartup()).isFalse();
    }

    @Test void localStartupDoesNotProvisionTopicsOrConsumeRecords() {
        assertThat(context.getBeanNamesForType(NewTopic.class)).isEmpty();
        assertThat(listeners.getListenerContainers()).isEmpty();
    }
}
