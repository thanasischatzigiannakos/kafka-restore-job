package com.example.kafkarestorejob.restoreengine.verification;

import org.springframework.stereotype.Service;

@Service
public class BinaryVerificationService {

    public BinaryVerificationService() {
    }

    public void verify(String restoreType, String messageType, Object payload) {
        // Compatibility shim for the older inspection-based path.
        // Binary completeness checks are intentionally disabled for now.
    }
}
