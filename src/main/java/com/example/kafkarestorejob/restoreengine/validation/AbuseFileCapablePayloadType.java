package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.serialization.model.AbuseRestoreMessage;
import org.springframework.stereotype.Component;

@Component
public class AbuseFileCapablePayloadType implements FileCapablePayloadType {

    @Override
    public Class<?> payloadClass() {
        return AbuseRestoreMessage.class;
    }
}
