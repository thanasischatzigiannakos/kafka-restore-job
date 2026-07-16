package com.example.kafkarestorejob.restoreengine.inspection;

import com.example.kafkarestorejob.restoreengine.verification.BinaryVerificationService;

public abstract class BinaryVerifyingRestoreMessageInspector<T>
        implements RestoreMessageInspector<T> {

    private final BinaryVerificationService binaryVerificationService;

    protected BinaryVerifyingRestoreMessageInspector(
            BinaryVerificationService binaryVerificationService
    ) {
        this.binaryVerificationService = binaryVerificationService;
    }

    @Override
    public void inspect(String restoreType, T payload) {
        binaryVerificationService.verify(restoreType, messageType(), payload);
    }
}
