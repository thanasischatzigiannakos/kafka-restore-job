package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.serialization.model.ApplicationRestoreMessage;
import com.example.kafkarestorejob.restoreengine.verification.FileReference;
import com.example.kafkarestorejob.restoreengine.verification.S3FileExistenceVerifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * Validates application payloads and extracts the currently supported application document
 * references that must exist in object storage.
 */
@Component
public class ApplicationMessageHandler extends BinaryMessageHandler<ApplicationRestoreMessage> {

    public ApplicationMessageHandler(S3FileExistenceVerifier s3FileExistenceVerifier) {
        super(s3FileExistenceVerifier);
    }

    /**
     * Returns the logical restore type handled by this validator.
     *
     * @return {@code application}
     */
    @Override
    public String getType() {
        return "application";
    }

    /**
     * Parses the placeholder application payload model.
     *
     * @param payloadBytes the serialized application payload
     * @return the parsed application payload
     */
    @Override
    protected ApplicationRestoreMessage parse(byte[] payloadBytes) {
        try {
            return ApplicationRestoreMessage.parseFrom(payloadBytes);
        } catch (Exception exception) {
            throw parsingFailure(exception);
        }
    }

    /**
     * Checks that the parsed payload advertises the expected application entity type.
     *
     * @param payload the parsed payload
     * @return {@code true} when the payload matches the application restore type
     */
    @Override
    protected boolean validateStructure(ApplicationRestoreMessage payload) {
        return payload.getEntityType() != null
                && "application".equalsIgnoreCase(payload.getEntityType());
    }

    /**
     * Builds a concise mismatch description for diagnostics.
     *
     * @param payload the parsed payload
     * @return the entity-type description
     */
    @Override
    protected String payloadDescription(ApplicationRestoreMessage payload) {
        return "entityType=" + payload.getEntityType();
    }

    /**
     * Extracts all currently supported application document references.
     *
     * @param payload the parsed payload
     * @return the collected file references
     */
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

    /**
     * Adds a file reference when the candidate document contains a non-blank object key.
     *
     * @param references the destination collection
     * @param fieldPath the logical field path used in diagnostics
     * @param document the candidate binary document
     */
    private void addIfPresent(
            List<FileReference> references,
            String fieldPath,
            ApplicationRestoreMessage.BinaryDocument document
    ) {
        if (document == null || document.getObjectKey() == null || document.getObjectKey().isBlank()) {
            return;
        }
        references.add(new FileReference(fieldPath, document.getObjectKey(), document.getChecksum()));
    }
}
