package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.serialization.model.AbuseRestoreMessage;
import com.example.kafkarestorejob.restoreengine.verification.FileReference;
import com.example.kafkarestorejob.restoreengine.verification.S3FileExistenceVerifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * Validates abuse payloads and extracts the currently supported binary references.
 */
@Component
public class AbuseMessageHandler extends BinaryMessageHandler<AbuseRestoreMessage> {

    public AbuseMessageHandler(S3FileExistenceVerifier s3FileExistenceVerifier) {
        super(s3FileExistenceVerifier);
    }

    /**
     * Returns the logical restore type handled by this validator.
     *
     * @return {@code abuse}
     */
    @Override
    public String getType() {
        return "abuse";
    }

    /**
     * Parses the placeholder abuse payload model.
     *
     * @param payloadBytes the serialized abuse payload
     * @return the parsed abuse payload
     */
    @Override
    protected AbuseRestoreMessage parse(byte[] payloadBytes) {
        try {
            return AbuseRestoreMessage.parseFrom(payloadBytes);
        } catch (Exception exception) {
            throw parsingFailure(exception);
        }
    }

    /**
     * Checks that the payload contains a usable message identifier.
     *
     * @param payload the parsed payload
     * @return {@code true} when the payload contains a non-blank message id
     */
    @Override
    protected boolean validateStructure(AbuseRestoreMessage payload) {
        return payload.getMessageId() != null && !payload.getMessageId().isBlank();
    }

    /**
     * Builds a concise mismatch description for diagnostics.
     *
     * @param payload the parsed payload
     * @return the message-id description
     */
    @Override
    protected String payloadDescription(AbuseRestoreMessage payload) {
        return "messageId=" + payload.getMessageId();
    }

    /**
     * Extracts all currently supported abuse file references.
     *
     * @param payload the parsed payload
     * @return the collected file references
     */
    @Override
    protected Collection<FileReference> extractFileReferences(AbuseRestoreMessage payload) {
        List<FileReference> references = new ArrayList<>();
        addIfPresent(references, "uploadedFile", payload.getUploadedFile());

        if (payload.getAttachments() != null) {
            for (int i = 0; i < payload.getAttachments().size(); i++) {
                addIfPresent(references, "attachments[" + i + "]", payload.getAttachments().get(i));
            }
        }

        return references;
    }

    /**
     * Adds a file reference when the uploaded-file entry contains a non-blank object key.
     *
     * @param references the destination collection
     * @param fieldPath the logical field path used in diagnostics
     * @param uploadedFile the candidate uploaded file
     */
    private void addIfPresent(
            List<FileReference> references,
            String fieldPath,
            AbuseRestoreMessage.UploadedFile uploadedFile
    ) {
        if (uploadedFile == null || uploadedFile.getObjectKey() == null || uploadedFile.getObjectKey().isBlank()) {
            return;
        }
        references.add(new FileReference(fieldPath, uploadedFile.getObjectKey(), uploadedFile.getChecksum()));
    }
}
