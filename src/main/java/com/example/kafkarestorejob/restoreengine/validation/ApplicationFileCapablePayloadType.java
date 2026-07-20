package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.serialization.model.ApplicationRestoreMessage;
import org.springframework.stereotype.Component;

@Component
public class ApplicationFileCapablePayloadType implements FileCapablePayloadType {

    @Override
    public Class<?> payloadClass() {
        return ApplicationRestoreMessage.class;
    }
}
