package com.example.kafkarestorejob.restoreengine.verification;

import com.example.kafkarestorejob.restoreengine.serialization.model.ApplicationRestoreMessage;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class ApplicationFileReferenceExtractor
        implements FileReferenceExtractor<ApplicationRestoreMessage> {

    @Override
    public Class<ApplicationRestoreMessage> payloadClass() {
        return ApplicationRestoreMessage.class;
    }

    @Override
    public Collection<FileReference> extract(ApplicationRestoreMessage payload) {
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
