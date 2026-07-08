package com.example.kafkarestorejob.restoreengine.serialization;

import com.example.kafkarestorejob.restoreengine.serialization.model.AbuseRestoreMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class AbuseRestoreMessageUnpacker extends AbstractJsonRestoreMessageUnpacker<AbuseRestoreMessage> {

    public AbuseRestoreMessageUnpacker(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    public String messageType() {
        return "abuse";
    }

    @Override
    public Class<AbuseRestoreMessage> payloadClass() {
        return AbuseRestoreMessage.class;
    }
}
