package com.example.kafkarestorejob.restoreengine.inspection;

import com.example.kafkarestorejob.restoreengine.serialization.model.ApplicationRestoreMessage;
import com.example.kafkarestorejob.restoreengine.verification.BinaryVerificationService;
import org.springframework.stereotype.Component;

@Component
public class ApplicationRestoreMessageInspector
        extends BinaryVerifyingRestoreMessageInspector<ApplicationRestoreMessage> {

    public ApplicationRestoreMessageInspector(
            BinaryVerificationService binaryVerificationService
    ) {
        super(binaryVerificationService);
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
