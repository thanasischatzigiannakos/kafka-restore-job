package com.example.kafkarestorejob.restoreengine.inspection;

import com.example.kafkarestorejob.restoreengine.serialization.model.AbuseRestoreMessage;
import com.example.kafkarestorejob.restoreengine.verification.BinaryVerificationService;
import org.springframework.stereotype.Component;

@Component
public class AbuseRestoreMessageInspector
        extends BinaryVerifyingRestoreMessageInspector<AbuseRestoreMessage> {

    public AbuseRestoreMessageInspector(
            BinaryVerificationService binaryVerificationService
    ) {
        super(binaryVerificationService);
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
