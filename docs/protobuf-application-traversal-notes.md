# Protobuf Application Traversal Notes

This repository still uses placeholder `ApplicationRestoreMessage` JSON-backed classes, but the real implementation will traverse generated protobuf classes such as `ApplicationType` and `UploadedDocType`.

## Recommended traversal shape

For protobuf-backed `ApplicationType`, the cheapest approach is:

1. Parse the Kafka value once with `ApplicationType.parseFrom(...)`.
2. Traverse the known repeated-field paths directly with indexed loops.
3. Validate S3 existence or checksum immediately when a usable uploaded-document reference is found.
4. Fail fast on the first binary validation error.

That avoids:

- parsing the same payload twice
- reflection-based protobuf walking
- flattening all document references into an intermediate collection
- traversing the structure once to collect and again to validate

## Example nested traversal

For a structure like:

```text
ApplicationType
  -> applicationDataForManualProcessingList[]
  -> additionalInfoDocRequest
  -> additionalDocList[]
  -> uploadedDocsList[]
```

the optimized traversal should look like this:

```java
for (int processingIndex = 0;
     processingIndex < application.getApplicationDataForManualProcessingCount();
     processingIndex++) {

    ApplicationDataForManualProcessingType processing =
            application.getApplicationDataForManualProcessing(processingIndex);

    if (!processing.hasAdditionalInfoDocRequest()) {
        continue;
    }

    AdditionalInfoDocRequestType request = processing.getAdditionalInfoDocRequest();

    for (int additionalDocIndex = 0;
         additionalDocIndex < request.getAdditionalDocCount();
         additionalDocIndex++) {

        AdditionalDocType additionalDoc = request.getAdditionalDoc(additionalDocIndex);

        for (int uploadedDocIndex = 0;
             uploadedDocIndex < additionalDoc.getUploadedDocsCount();
             uploadedDocIndex++) {

            UploadedDocType uploadedDoc = additionalDoc.getUploadedDocs(uploadedDocIndex);

            validateUploadedDocument(
                    uploadedDoc,
                    "applicationDataForManualProcessing[" + processingIndex + "]"
                            + ".additionalInfoDocRequest"
                            + ".additionalDoc[" + additionalDocIndex + "]"
                            + ".uploadedDocs[" + uploadedDocIndex + "]"
            );
        }
    }
}
```

The same pattern applies to nested list-of-list structures. Use:

- `hasX()` for optional singular protobuf message fields
- `getXCount()` plus `getX(index)` for repeated fields
- wrapper extraction plus blank checks for string-like identifiers

## Uploaded-document validation shape

For real protobuf `UploadedDocType`, keep one focused helper:

```java
private void validateUploadedDocument(
        UploadedDocType uploadedDoc,
        String fieldPath
) {
    Optional<String> binaryId = optionalString(
            uploadedDoc.hasBinaryId(),
            uploadedDoc.getBinaryId()
    );
    Optional<String> applicantUploadedFile = optionalString(
            uploadedDoc.hasApplicantUploadedFile(),
            uploadedDoc.getApplicantUploadedFile()
    );
    Optional<String> checksum = optionalString(
            uploadedDoc.hasChecksum(),
            uploadedDoc.getChecksum()
    );
    boolean hasEmbeddedBinary = !uploadedDoc.getApplicantUploadedFileBinary().isEmpty();

    if (hasEmbeddedBinary) {
        // Embedded-byte semantics should be confirmed from the real producer/upload flow.
        // If embedded bytes are authoritative, validate them in-memory and skip S3.
        return;
    }

    String objectKey = resolveObjectKey(binaryId, applicantUploadedFile, fieldPath);
    binaryObjectValidator.validate(objectKey, checksum, fieldPath);

    for (int translationIndex = 0;
         translationIndex < uploadedDoc.getTranslationCount();
         translationIndex++) {
        validateTranslation(
                uploadedDoc.getTranslation(translationIndex),
                fieldPath + ".translation[" + translationIndex + "]"
        );
    }
}
```

## S3 key mapping guidance

The real implementation must confirm whether `binaryId` or `applicantUploadedFile` is the S3 object key by checking the actual upload or persistence path.

Do not guess that mapping.

Until the protobuf library and upload service are present in this repository, the current placeholder implementation continues using the existing `objectKey` field exposed by the local model classes.

## Checksum guidance

The current placeholder implementation in this repository assumes SHA-256:

- missing checksum: existence-only validation
- present checksum: compare using S3 `checksumSHA256` when available
- fallback: stream the object and calculate SHA-256 locally
- accepted expected-checksum encodings: Base64 or hex, with optional `sha256:` or `sha-256:` prefix

For the real protobuf implementation, keep the same structure but confirm the checksum algorithm and encoding from the actual upload path before relying on them.
