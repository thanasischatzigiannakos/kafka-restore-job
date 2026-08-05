package com.example.kafkarestorejob.restoreengine.serialization.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Placeholder notification payload model used by the JSON-backed test implementation.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class NotificationRestoreMessage {

    /**
     * Parses the serialized placeholder payload from JSON bytes.
     *
     * @param payloadBytes the serialized payload bytes
     * @return the parsed notification payload
     */
    public static NotificationRestoreMessage parseFrom(byte[] payloadBytes) {
        return JsonPayloadParser.parse(payloadBytes, NotificationRestoreMessage.class);
    }

    private String messageId;
    private String entityType;
    private String recipientId;
    private String notificationStatus;

    /**
     * Returns the notification message identifier.
     *
     * @return the message identifier
     */
    public String getMessageId() {
        return messageId;
    }

    /**
     * Sets the notification message identifier.
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
     * Returns the recipient identifier.
     *
     * @return the recipient identifier
     */
    public String getRecipientId() {
        return recipientId;
    }

    /**
     * Sets the recipient identifier.
     *
     * @param recipientId the recipient identifier
     */
    public void setRecipientId(String recipientId) {
        this.recipientId = recipientId;
    }

    /**
     * Returns the notification status.
     *
     * @return the notification status
     */
    public String getNotificationStatus() {
        return notificationStatus;
    }

    /**
     * Sets the notification status.
     *
     * @param notificationStatus the notification status
     */
    public void setNotificationStatus(String notificationStatus) {
        this.notificationStatus = notificationStatus;
    }
}
