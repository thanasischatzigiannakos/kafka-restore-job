package com.example.kafkarestorejob.restoreengine.verification;

import com.example.kafkarestorejob.restoreengine.serialization.model.AbuseRestoreMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class AbuseBinaryReferenceExtractor implements FileReferenceExtractor<AbuseRestoreMessage> {

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
                Optional.empty(),
                Optional.ofNullable(file.getChecksum()),
                BinaryReferencePurpose.REQUIRED_CONTENT
        ));
    }
}
