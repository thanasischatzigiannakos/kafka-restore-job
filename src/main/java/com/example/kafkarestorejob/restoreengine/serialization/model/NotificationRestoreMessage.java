package com.example.kafkarestorejob.restoreengine.serialization.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NotificationRestoreMessage {

    public static NotificationRestoreMessage parseFrom(byte[] payloadBytes) {
        return JsonPayloadParser.parse(payloadBytes, NotificationRestoreMessage.class);
    }

    private String messageId;
    private String entityType;
    private String recipientId;
    private String notificationStatus;

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

    public String getRecipientId() {
        return recipientId;
    }

    public void setRecipientId(String recipientId) {
        this.recipientId = recipientId;
    }

    public String getNotificationStatus() {
        return notificationStatus;
    }

    public void setNotificationStatus(String notificationStatus) {
        this.notificationStatus = notificationStatus;
    }
}
