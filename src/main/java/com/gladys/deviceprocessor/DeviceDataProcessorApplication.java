package com.gladys.deviceprocessor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class DeviceDataProcessorApplication {
    public static void main(String[] args) {
        SpringApplication.run(DeviceDataProcessorApplication.class, args);
    }
}
