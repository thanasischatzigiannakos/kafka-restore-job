package com.example.kafkarestorejob.restoreengine.serialization.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ApplicationRestoreMessage {

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

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public Integer getSequence() {
        return sequence;
    }

    public void setSequence(Integer sequence) {
        this.sequence = sequence;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public ApplicationPayload getPayload() {
        return payload;
    }

    public void setPayload(ApplicationPayload payload) {
        this.payload = payload;
    }

    public WrittenDocument getWrittenDocument() {
        return writtenDocument;
    }

    public void setWrittenDocument(WrittenDocument writtenDocument) {
        this.writtenDocument = writtenDocument;
    }

    public Applicant getApplicant() {
        return applicant;
    }

    public void setApplicant(Applicant applicant) {
        this.applicant = applicant;
    }

    public List<TranslatedFile> getTranslatedFiles() {
        return translatedFiles;
    }

    public void setTranslatedFiles(List<TranslatedFile> translatedFiles) {
        this.translatedFiles = translatedFiles;
    }

    public SignedForm getSignedForm() {
        return signedForm;
    }

    public void setSignedForm(SignedForm signedForm) {
        this.signedForm = signedForm;
    }

    public SignedLocallyForm getSignedLocallyForm() {
        return signedLocallyForm;
    }

    public void setSignedLocallyForm(SignedLocallyForm signedLocallyForm) {
        this.signedLocallyForm = signedLocallyForm;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ApplicationPayload {

        private String applicantId;
        private String documentId;
        private String status;
        private String notes;

        public String getApplicantId() {
            return applicantId;
        }

        public void setApplicantId(String applicantId) {
            this.applicantId = applicantId;
        }

        public String getDocumentId() {
            return documentId;
        }

        public void setDocumentId(String documentId) {
            this.documentId = documentId;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getNotes() {
            return notes;
        }

        public void setNotes(String notes) {
            this.notes = notes;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Applicant {

        private List<ApplicantUploadedFile> uploadedFiles;

        public List<ApplicantUploadedFile> getUploadedFiles() {
            return uploadedFiles;
        }

        public void setUploadedFiles(List<ApplicantUploadedFile> uploadedFiles) {
            this.uploadedFiles = uploadedFiles;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BinaryDocument {

        private String bucketKey;
        private String objectKey;
        private String checksum;

        public String getBucketKey() {
            return bucketKey;
        }

        public void setBucketKey(String bucketKey) {
            this.bucketKey = bucketKey;
        }

        public String getObjectKey() {
            return objectKey;
        }

        public void setObjectKey(String objectKey) {
            this.objectKey = objectKey;
        }

        public String getChecksum() {
            return checksum;
        }

        public void setChecksum(String checksum) {
            this.checksum = checksum;
        }
    }

    public static class WrittenDocument extends BinaryDocument { }

    public static class ApplicantUploadedFile extends BinaryDocument { }

    public static class TranslatedFile extends BinaryDocument { }

    public static class SignedForm extends BinaryDocument { }

    public static class SignedLocallyForm extends BinaryDocument { }
}
