package com.example.kafkarestorejob.restoreengine.verification;

import com.example.kafkarestorejob.restoreengine.serialization.model.AbuseRestoreMessage;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class AbuseFileReferenceExtractor implements FileReferenceExtractor<AbuseRestoreMessage> {

    @Override
    public Class<AbuseRestoreMessage> payloadClass() {
        return AbuseRestoreMessage.class;
    }

    @Override
    public Collection<FileReference> extract(AbuseRestoreMessage payload) {
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
