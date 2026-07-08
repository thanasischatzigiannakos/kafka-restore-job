package com.example.kafkarestorejob.restoreengine.verification;

import com.example.kafkarestorejob.restoreengine.serialization.model.AbuseRestoreMessage;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class AbuseBinaryReferenceExtractor implements BinaryReferenceExtractor<AbuseRestoreMessage> {

    @Override
    public String messageType() {
        return "abuse";
    }

    @Override
    public Class<AbuseRestoreMessage> payloadClass() {
        return AbuseRestoreMessage.class;
    }

    @Override
    public List<BinaryReference> extract(AbuseRestoreMessage payload) {
        List<BinaryReference> references = new ArrayList<>();
        addIfPresent(references, "uploadedFile", payload.getUploadedFile());

        if (payload.getAttachments() != null) {
            for (int i = 0; i < payload.getAttachments().size(); i++) {
                addIfPresent(references, "attachments[" + i + "]", payload.getAttachments().get(i));
            }
        }

        return references;
    }

    private void addIfPresent(
            List<BinaryReference> references,
            String fieldPath,
            AbuseRestoreMessage.UploadedFile file
    ) {
        if (file == null || file.getObjectKey() == null || file.getObjectKey().isBlank()) {
            return;
        }

        references.add(new BinaryReference(
                fieldPath,
                file.getBucketKey(),
                file.getObjectKey(),
                file.getChecksum()
        ));
    }
}
