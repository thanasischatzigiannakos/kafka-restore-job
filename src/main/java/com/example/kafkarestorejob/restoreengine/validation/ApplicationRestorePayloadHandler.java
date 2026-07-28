package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.serialization.model.ApplicationRestoreMessage;
import com.example.kafkarestorejob.restoreengine.verification.FileReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class ApplicationRestorePayloadHandler
        extends AbstractParsedPayloadHandler<ApplicationRestoreMessage> {

    public ApplicationRestorePayloadHandler() {
    }

    @Override
    public String restoreType() {
        return "application";
    }

    @Override
    protected ApplicationRestoreMessage parse(byte[] payloadBytes) {
        try {
            return ApplicationRestoreMessage.parseFrom(payloadBytes);
        } catch (Exception exception) {
            throw parsingFailure(exception);
        }
    }

    @Override
    protected void validatePayloadType(
            RestoreRecordValidationContext context,
            ApplicationRestoreMessage payload
    ) {
        if (payload.getEntityType() == null || !"application".equalsIgnoreCase(payload.getEntityType())) {
            throw new RestorePayloadValidationException(
                    "Unexpected payload type for restoreType=" + context.restoreType()
                            + " sourceTopic=" + context.sourceTopic()
                            + " partition=" + context.partition()
                            + " offset=" + context.offset()
                            + " expectedMessageType=application"
                            + " actualEntityType=" + payload.getEntityType()
            );
        }
    }

    @Override
    protected Collection<FileReference> extractFileReferences(ApplicationRestoreMessage payload) {
        List<FileReference> references = new ArrayList<>();
        addIfPresent(references, "writtenDocument", payload.getWrittenDocument());
        addIfPresent(references, "signedForm", payload.getSignedForm());
        addIfPresent(references, "signedLocallyForm", payload.getSignedLocallyForm());

        if (payload.getTranslatedFiles() != null) {
            for (int i = 0; i < payload.getTranslatedFiles().size(); i++) {
                addIfPresent(references, "translatedFiles[" + i + "]", payload.getTranslatedFiles().get(i));
            }
        }

        if (payload.getApplicant() != null && payload.getApplicant().getUploadedFiles() != null) {
            for (int i = 0; i < payload.getApplicant().getUploadedFiles().size(); i++) {
                addIfPresent(
                        references,
                        "applicant.uploadedFiles[" + i + "]",
                        payload.getApplicant().getUploadedFiles().get(i)
                );
            }
        }

        return references;
    }

    private void addIfPresent(
            List<FileReference> references,
            String fieldPath,
            ApplicationRestoreMessage.BinaryDocument document
    ) {
        if (document == null || document.getObjectKey() == null || document.getObjectKey().isBlank()) {
            return;
        }
        references.add(new FileReference(fieldPath, document.getObjectKey()));
    }
}
