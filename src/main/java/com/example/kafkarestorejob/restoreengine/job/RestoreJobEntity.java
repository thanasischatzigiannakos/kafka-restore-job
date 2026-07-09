package com.example.kafkarestorejob.restoreengine.job;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "restore_jobs")
public class RestoreJobEntity {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String restoreType;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private RestoreJobStatus status;

    @Column(nullable = false)
    private Instant requestedAt;

    private Instant startedAt;

    private Instant completedAt;

    private Instant cancellationRequestedAt;

    private Instant restoreFromTimestamp;

    @Column(nullable = false)
    private Instant updatedAt;

    private String sourceTopic;

    private String targetTopic;

    private String messageType;

    private String errorMessage;

    private Integer batchesCommitted;

    private Long recordsRestored;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getRestoreType() {
        return restoreType;
    }

    public void setRestoreType(String restoreType) {
        this.restoreType = restoreType;
    }

    public RestoreJobStatus getStatus() {
        return status;
    }

    public void setStatus(RestoreJobStatus status) {
        this.status = status;
    }

    public Instant getRequestedAt() {
        return requestedAt;
    }

    public void setRequestedAt(Instant requestedAt) {
        this.requestedAt = requestedAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
    }

    public Instant getCancellationRequestedAt() {
        return cancellationRequestedAt;
    }

    public void setCancellationRequestedAt(Instant cancellationRequestedAt) {
        this.cancellationRequestedAt = cancellationRequestedAt;
    }

    public Instant getRestoreFromTimestamp() {
        return restoreFromTimestamp;
    }

    public void setRestoreFromTimestamp(Instant restoreFromTimestamp) {
        this.restoreFromTimestamp = restoreFromTimestamp;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public void setSourceTopic(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    public String getTargetTopic() {
        return targetTopic;
    }

    public void setTargetTopic(String targetTopic) {
        this.targetTopic = targetTopic;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public Integer getBatchesCommitted() {
        return batchesCommitted;
    }

    public void setBatchesCommitted(Integer batchesCommitted) {
        this.batchesCommitted = batchesCommitted;
    }

    public Long getRecordsRestored() {
        return recordsRestored;
    }

    public void setRecordsRestored(Long recordsRestored) {
        this.recordsRestored = recordsRestored;
    }

    @PrePersist
    void onCreate() {
        updatedAt = Instant.now();
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = Instant.now();
    }
}
