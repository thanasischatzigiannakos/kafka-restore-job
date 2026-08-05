package com.example.kafkarestorejob.restoreengine.serialization.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * Placeholder application payload model used by the JSON-backed implementation in this
 * repository.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ApplicationRestoreMessage {

    /**
     * Parses the serialized placeholder payload from JSON bytes.
     *
     * @param payloadBytes the serialized payload bytes
     * @return the parsed application payload
     */
    public static ApplicationRestoreMessage parseFrom(byte[] payloadBytes) {
        return JsonPayloadParser.parse(payloadBytes, ApplicationRestoreMessage.class);
    }

    private String messageId;
    private String entityType;
    private Integer sequence;
    private String createdAt;
    private ApplicationPayload payload;
    private WrittenDocument writtenDocument;
    private Applicant applicant;
    private List<TranslatedFile> translatedFiles;
    private SignedForm signedForm;
    private SignedLocallyForm signedLocallyForm;

    /**
     * Returns the message identifier.
     *
     * @return the message identifier
     */
    public String getMessageId() {
        return messageId;
    }

    /**
     * Sets the message identifier.
     *
     * @param messageId the message identifier
     */
    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    /**
     * Returns the logical entity type encoded in the payload.
     *
     * @return the entity type
     */
    public String getEntityType() {
        return entityType;
    }

    /**
     * Sets the logical entity type encoded in the payload.
     *
     * @param entityType the entity type
     */
    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    /**
     * Returns the sequence number, when present.
     *
     * @return the sequence number
     */
    public Integer getSequence() {
        return sequence;
    }

    /**
     * Sets the sequence number.
     *
     * @param sequence the sequence number
     */
    public void setSequence(Integer sequence) {
        this.sequence = sequence;
    }

    /**
     * Returns the creation timestamp encoded in the payload.
     *
     * @return the creation timestamp text
     */
    public String getCreatedAt() {
        return createdAt;
    }

    /**
     * Sets the creation timestamp encoded in the payload.
     *
     * @param createdAt the creation timestamp text
     */
    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    /**
     * Returns the nested application payload block.
     *
     * @return the nested payload block
     */
    public ApplicationPayload getPayload() {
        return payload;
    }

    /**
     * Sets the nested application payload block.
     *
     * @param payload the nested payload block
     */
    public void setPayload(ApplicationPayload payload) {
        this.payload = payload;
    }

    /**
     * Returns the written document reference.
     *
     * @return the written document
     */
    public WrittenDocument getWrittenDocument() {
        return writtenDocument;
    }

    /**
     * Sets the written document reference.
     *
     * @param writtenDocument the written document
     */
    public void setWrittenDocument(WrittenDocument writtenDocument) {
        this.writtenDocument = writtenDocument;
    }

    /**
     * Returns the applicant block.
     *
     * @return the applicant block
     */
    public Applicant getApplicant() {
        return applicant;
    }

    /**
     * Sets the applicant block.
     *
     * @param applicant the applicant block
     */
    public void setApplicant(Applicant applicant) {
        this.applicant = applicant;
    }

    /**
     * Returns the translated-file references.
     *
     * @return the translated-file references
     */
    public List<TranslatedFile> getTranslatedFiles() {
        return translatedFiles;
    }

    /**
     * Sets the translated-file references.
     *
     * @param translatedFiles the translated-file references
     */
    public void setTranslatedFiles(List<TranslatedFile> translatedFiles) {
        this.translatedFiles = translatedFiles;
    }

    /**
     * Returns the signed form reference.
     *
     * @return the signed form
     */
    public SignedForm getSignedForm() {
        return signedForm;
    }

    /**
     * Sets the signed form reference.
     *
     * @param signedForm the signed form
     */
    public void setSignedForm(SignedForm signedForm) {
        this.signedForm = signedForm;
    }

    /**
     * Returns the locally signed form reference.
     *
     * @return the locally signed form
     */
    public SignedLocallyForm getSignedLocallyForm() {
        return signedLocallyForm;
    }

    /**
     * Sets the locally signed form reference.
     *
     * @param signedLocallyForm the locally signed form
     */
    public void setSignedLocallyForm(SignedLocallyForm signedLocallyForm) {
        this.signedLocallyForm = signedLocallyForm;
    }

    /**
     * Placeholder nested application payload details.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ApplicationPayload {

        private String applicantId;
        private String documentId;
        private String status;
        private String notes;

        /**
         * Returns the applicant identifier.
         *
         * @return the applicant identifier
         */
        public String getApplicantId() {
            return applicantId;
        }

        /**
         * Sets the applicant identifier.
         *
         * @param applicantId the applicant identifier
         */
        public void setApplicantId(String applicantId) {
            this.applicantId = applicantId;
        }

        /**
         * Returns the document identifier.
         *
         * @return the document identifier
         */
        public String getDocumentId() {
            return documentId;
        }

        /**
         * Sets the document identifier.
         *
         * @param documentId the document identifier
         */
        public void setDocumentId(String documentId) {
            this.documentId = documentId;
        }

        /**
         * Returns the application status.
         *
         * @return the status
         */
        public String getStatus() {
            return status;
        }

        /**
         * Sets the application status.
         *
         * @param status the status
         */
        public void setStatus(String status) {
            this.status = status;
        }

        /**
         * Returns free-form notes associated with the payload.
         *
         * @return the notes
         */
        public String getNotes() {
            return notes;
        }

        /**
         * Sets free-form notes associated with the payload.
         *
         * @param notes the notes
         */
        public void setNotes(String notes) {
            this.notes = notes;
        }
    }

    /**
     * Placeholder applicant block containing uploaded-file references.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Applicant {

        private List<ApplicantUploadedFile> uploadedFiles;

        /**
         * Returns the applicant uploaded files.
         *
         * @return the applicant uploaded files
         */
        public List<ApplicantUploadedFile> getUploadedFiles() {
            return uploadedFiles;
        }

        /**
         * Sets the applicant uploaded files.
         *
         * @param uploadedFiles the applicant uploaded files
         */
        public void setUploadedFiles(List<ApplicantUploadedFile> uploadedFiles) {
            this.uploadedFiles = uploadedFiles;
        }
    }

    /**
     * Base placeholder document model carrying object-storage reference data.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BinaryDocument {

        private String bucketKey;
        private String objectKey;
        private String checksum;

        /**
         * Returns the optional bucket key.
         *
         * @return the bucket key
         */
        public String getBucketKey() {
            return bucketKey;
        }

        /**
         * Sets the optional bucket key.
         *
         * @param bucketKey the bucket key
         */
        public void setBucketKey(String bucketKey) {
            this.bucketKey = bucketKey;
        }

        /**
         * Returns the object key.
         *
         * @return the object key
         */
        public String getObjectKey() {
            return objectKey;
        }

        /**
         * Sets the object key.
         *
         * @param objectKey the object key
         */
        public void setObjectKey(String objectKey) {
            this.objectKey = objectKey;
        }

        /**
         * Returns the optional checksum.
         *
         * @return the checksum
         */
        public String getChecksum() {
            return checksum;
        }

        /**
         * Sets the optional checksum.
         *
         * @param checksum the checksum
         */
        public void setChecksum(String checksum) {
            this.checksum = checksum;
        }
    }

    /**
     * Placeholder written-document reference.
     */
    public static class WrittenDocument extends BinaryDocument { }

    /**
     * Placeholder applicant-uploaded-file reference.
     */
    public static class ApplicantUploadedFile extends BinaryDocument { }

    /**
     * Placeholder translated-file reference.
     */
    public static class TranslatedFile extends BinaryDocument { }

    /**
     * Placeholder signed-form reference.
     */
    public static class SignedForm extends BinaryDocument { }

    /**
     * Placeholder locally signed-form reference.
     */
    public static class SignedLocallyForm extends BinaryDocument { }
}
