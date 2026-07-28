package com.example.kafkarestorejob.restoreengine.serialization.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AbuseRestoreMessage {

    public static AbuseRestoreMessage parseFrom(byte[] payloadBytes) {
        return JsonPayloadParser.parse(payloadBytes, AbuseRestoreMessage.class);
    }

    private String messageId;
    private String caseId;
    private AbusePayload payload;
    private UploadedFile uploadedFile;
    private List<UploadedFile> attachments;

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getCaseId() {
        return caseId;
    }

    public void setCaseId(String caseId) {
        this.caseId = caseId;
    }

    public AbusePayload getPayload() {
        return payload;
    }

    public void setPayload(AbusePayload payload) {
        this.payload = payload;
    }

    public UploadedFile getUploadedFile() {
        return uploadedFile;
    }

    public void setUploadedFile(UploadedFile uploadedFile) {
        this.uploadedFile = uploadedFile;
    }

    public List<UploadedFile> getAttachments() {
        return attachments;
    }

    public void setAttachments(List<UploadedFile> attachments) {
        this.attachments = attachments;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AbusePayload {

        private String reporterId;
        private String reason;

        public String getReporterId() {
            return reporterId;
        }

        public void setReporterId(String reporterId) {
            this.reporterId = reporterId;
        }

        public String getReason() {
            return reason;
        }

        public void setReason(String reason) {
            this.reason = reason;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class UploadedFile {

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
}
