package com.example.kafkarestorejob.restoreengine.validation;

import com.example.kafkarestorejob.restoreengine.serialization.model.AbuseRestoreMessage;
import com.example.kafkarestorejob.restoreengine.verification.FileReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class AbuseRestorePayloadHandler
        extends AbstractJsonRestorePayloadHandler<AbuseRestoreMessage> {

    public AbuseRestorePayloadHandler(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    public String restoreType() {
        return "abuse";
    }

    @Override
    protected Class<AbuseRestoreMessage> payloadClass() {
        return AbuseRestoreMessage.class;
    }

    @Override
    protected void validatePayloadType(
            RestoreRecordValidationContext context,
            AbuseRestoreMessage payload
    ) {
        if (payload.getMessageId() == null || payload.getMessageId().isBlank()) {
            throw new RestorePayloadValidationException(
                    "Unexpected payload type for restoreType=" + context.restoreType()
                            + " sourceTopic=" + context.sourceTopic()
                            + " partition=" + context.partition()
                            + " offset=" + context.offset()
                            + " expectedMessageType=abuse"
                            + " actualMessageId=" + payload.getMessageId()
            );
        }
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
        references.add(new FileReference(fieldPath, uploadedFile.getObjectKey()));
    }
}
