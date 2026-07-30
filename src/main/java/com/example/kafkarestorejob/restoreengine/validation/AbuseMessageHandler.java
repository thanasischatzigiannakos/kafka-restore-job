package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.serialization.model.AbuseRestoreMessage;
import com.example.kafkarestorejob.restoreengine.verification.FileReference;
import com.example.kafkarestorejob.restoreengine.verification.S3FileExistenceVerifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class AbuseMessageHandler extends BinaryMessageHandler<AbuseRestoreMessage> {

    public AbuseMessageHandler(S3FileExistenceVerifier s3FileExistenceVerifier) {
        super(s3FileExistenceVerifier);
    }

    @Override
    public String getType() {
        return "abuse";
    }

    @Override
    protected AbuseRestoreMessage parse(byte[] payloadBytes) {
        try {
            return AbuseRestoreMessage.parseFrom(payloadBytes);
        } catch (Exception exception) {
            throw parsingFailure(exception);
        }
    }

    @Override
    protected boolean validateStructure(AbuseRestoreMessage payload) {
        return payload.getMessageId() != null && !payload.getMessageId().isBlank();
    }

    @Override
    protected String payloadDescription(AbuseRestoreMessage payload) {
        return "messageId=" + payload.getMessageId();
    }

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
