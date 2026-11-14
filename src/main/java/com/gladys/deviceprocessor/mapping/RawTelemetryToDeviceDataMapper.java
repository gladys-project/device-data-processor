package com.gladys.deviceprocessor.mapping;

import java.util.List;
import com.gladys.deviceprocessor.message.DeviceError;
import com.gladys.deviceprocessor.message.KafkaDeviceData;
import com.gladys.deviceprocessor.message.KafkaRawTelemetry;
import org.mapstruct.BeanMapping;
import org.mapstruct.InjectionStrategy;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;
import org.springframework.messaging.Message;

/** Projects device diagnostics and correlation; does not interpret sensor measurements. */
@Mapper(componentModel = MappingConstants.ComponentModel.SPRING,
    injectionStrategy = InjectionStrategy.CONSTRUCTOR,
    unmappedSourcePolicy = ReportingPolicy.ERROR,
    unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RawTelemetryToDeviceDataMapper {
    @BeanMapping(ignoreUnmappedSourceProperties = "payload")
    KafkaDeviceData toDeviceData(KafkaRawTelemetry source);

    List<DeviceError> copyErrors(List<DeviceError> source);

    DeviceError copyError(DeviceError source);
}
