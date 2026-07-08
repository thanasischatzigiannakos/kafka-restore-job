package com.example.kafkarestorejob.restoreengine.serialization;

import com.example.kafkarestorejob.restoreengine.serialization.model.ApplicationRestoreMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class ApplicationRestoreMessageUnpacker extends AbstractJsonRestoreMessageUnpacker<ApplicationRestoreMessage> {

    public ApplicationRestoreMessageUnpacker(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    public String messageType() {
        return "application";
    }

    @Override
    public Class<ApplicationRestoreMessage> payloadClass() {
        return ApplicationRestoreMessage.class;
    }
}
