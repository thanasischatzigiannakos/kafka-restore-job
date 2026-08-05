package com.example.kafkarestorejob.restoreengine.serialization.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * Placeholder abuse payload model used by the JSON-backed implementation in this repository.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AbuseRestoreMessage {

    /**
     * Parses the serialized placeholder payload from JSON bytes.
     *
     * @param payloadBytes the serialized payload bytes
     * @return the parsed abuse payload
     */
    public static AbuseRestoreMessage parseFrom(byte[] payloadBytes) {
        return JsonPayloadParser.parse(payloadBytes, AbuseRestoreMessage.class);
    }

    private String messageId;
    private String caseId;
    private AbusePayload payload;
    private UploadedFile uploadedFile;
    private List<UploadedFile> attachments;

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
     * Returns the abuse case identifier.
     *
     * @return the case identifier
     */
    public String getCaseId() {
        return caseId;
    }

    /**
     * Sets the abuse case identifier.
     *
     * @param caseId the case identifier
     */
    public void setCaseId(String caseId) {
        this.caseId = caseId;
    }

    /**
     * Returns the nested abuse payload block.
     *
     * @return the nested payload block
     */
    public AbusePayload getPayload() {
        return payload;
    }

    /**
     * Sets the nested abuse payload block.
     *
     * @param payload the nested payload block
     */
    public void setPayload(AbusePayload payload) {
        this.payload = payload;
    }

    /**
     * Returns the primary uploaded-file reference.
     *
     * @return the uploaded-file reference
     */
    public UploadedFile getUploadedFile() {
        return uploadedFile;
    }

    /**
     * Sets the primary uploaded-file reference.
     *
     * @param uploadedFile the uploaded-file reference
     */
    public void setUploadedFile(UploadedFile uploadedFile) {
        this.uploadedFile = uploadedFile;
    }

    /**
     * Returns the attachment references.
     *
     * @return the attachment references
     */
    public List<UploadedFile> getAttachments() {
        return attachments;
    }

    /**
     * Sets the attachment references.
     *
     * @param attachments the attachment references
     */
    public void setAttachments(List<UploadedFile> attachments) {
        this.attachments = attachments;
    }

    /**
     * Placeholder nested abuse payload details.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AbusePayload {

        private String reporterId;
        private String reason;

        /**
         * Returns the reporter identifier.
         *
         * @return the reporter identifier
         */
        public String getReporterId() {
            return reporterId;
        }

        /**
         * Sets the reporter identifier.
         *
         * @param reporterId the reporter identifier
         */
        public void setReporterId(String reporterId) {
            this.reporterId = reporterId;
        }

        /**
         * Returns the abuse reason.
         *
         * @return the abuse reason
         */
        public String getReason() {
            return reason;
        }

        /**
         * Sets the abuse reason.
         *
         * @param reason the abuse reason
         */
        public void setReason(String reason) {
            this.reason = reason;
        }
    }

    /**
     * Placeholder uploaded-file block carrying object-storage reference data.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class UploadedFile {

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
}
