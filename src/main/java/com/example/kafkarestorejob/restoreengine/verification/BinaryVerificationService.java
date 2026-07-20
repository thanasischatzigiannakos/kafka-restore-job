package com.example.kafkarestorejob.restoreengine.verification;

import com.example.kafkarestorejob.restoreengine.validation.RestoreRecordValidationContext;
import java.util.Collection;
import org.springframework.stereotype.Service;

@Service
public class BinaryVerificationService {

    private final FileReferenceExtractorRegistry extractorRegistry;
    private final BinaryCompletenessValidator binaryCompletenessValidator;

    public BinaryVerificationService(
            FileReferenceExtractorRegistry extractorRegistry,
            BinaryCompletenessValidator binaryCompletenessValidator
    ) {
        this.extractorRegistry = extractorRegistry;
        this.binaryCompletenessValidator = binaryCompletenessValidator;
    }

    public void verify(String restoreType, String messageType, Object payload) {
        if (payload == null) {
            return;
        }

        Class<?> payloadClass = payload.getClass();
        if (!extractorRegistry.supports(payloadClass)) {
            return;
        }

        Collection<BinaryReference> references = extractorRegistry.extract(payloadClass, payload);
        RestoreRecordValidationContext context =
                new RestoreRecordValidationContext(null, restoreType, "unknown", -1, -1L);
        for (BinaryReference reference : references) {
            binaryCompletenessValidator.validate(context, messageType, payloadClass, reference);
        }
    }
}
