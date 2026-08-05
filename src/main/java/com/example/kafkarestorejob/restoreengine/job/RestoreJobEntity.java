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

/**
 * Persisted representation of a restore job and its most recent execution metadata.
 */
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

    private Integer committedTransactions;

    private Long recordsRestored;

    /**
     * Returns the restore job identifier.
     *
     * @return the job identifier
     */
    public UUID getId() {
        return id;
    }

    /**
     * Sets the restore job identifier.
     *
     * @param id the job identifier
     */
    public void setId(UUID id) {
        this.id = id;
    }

    /**
     * Returns the logical restore type.
     *
     * @return the restore type key
     */
    public String getRestoreType() {
        return restoreType;
    }

    /**
     * Sets the logical restore type.
     *
     * @param restoreType the restore type key
     */
    public void setRestoreType(String restoreType) {
        this.restoreType = restoreType;
    }

    /**
     * Returns the current persisted job status.
     *
     * @return the job status
     */
    public RestoreJobStatus getStatus() {
        return status;
    }

    /**
     * Sets the current persisted job status.
     *
     * @param status the job status
     */
    public void setStatus(RestoreJobStatus status) {
        this.status = status;
    }

    /**
     * Returns the timestamp at which the job was requested.
     *
     * @return the request timestamp
     */
    public Instant getRequestedAt() {
        return requestedAt;
    }

    /**
     * Sets the timestamp at which the job was requested.
     *
     * @param requestedAt the request timestamp
     */
    public void setRequestedAt(Instant requestedAt) {
        this.requestedAt = requestedAt;
    }

    /**
     * Returns the timestamp at which execution started.
     *
     * @return the start timestamp
     */
    public Instant getStartedAt() {
        return startedAt;
    }

    /**
     * Sets the timestamp at which execution started.
     *
     * @param startedAt the start timestamp
     */
    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    /**
     * Returns the timestamp at which execution completed or was cancelled.
     *
     * @return the completion timestamp
     */
    public Instant getCompletedAt() {
        return completedAt;
    }

    /**
     * Sets the timestamp at which execution completed or was cancelled.
     *
     * @param completedAt the completion timestamp
     */
    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
    }

    /**
     * Returns the timestamp at which cancellation was requested.
     *
     * @return the cancellation-request timestamp
     */
    public Instant getCancellationRequestedAt() {
        return cancellationRequestedAt;
    }

    /**
     * Sets the timestamp at which cancellation was requested.
     *
     * @param cancellationRequestedAt the cancellation-request timestamp
     */
    public void setCancellationRequestedAt(Instant cancellationRequestedAt) {
        this.cancellationRequestedAt = cancellationRequestedAt;
    }

    /**
     * Returns the optional timestamp used to position the source consumer.
     *
     * @return the restore-from timestamp
     */
    public Instant getRestoreFromTimestamp() {
        return restoreFromTimestamp;
    }

    /**
     * Sets the optional timestamp used to position the source consumer.
     *
     * @param restoreFromTimestamp the restore-from timestamp
     */
    public void setRestoreFromTimestamp(Instant restoreFromTimestamp) {
        this.restoreFromTimestamp = restoreFromTimestamp;
    }

    /**
     * Returns the timestamp of the most recent entity update.
     *
     * @return the update timestamp
     */
    public Instant getUpdatedAt() {
        return updatedAt;
    }

    /**
     * Sets the timestamp of the most recent entity update.
     *
     * @param updatedAt the update timestamp
     */
    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    /**
     * Returns the source topic captured for the execution.
     *
     * @return the source topic
     */
    public String getSourceTopic() {
        return sourceTopic;
    }

    /**
     * Sets the source topic captured for the execution.
     *
     * @param sourceTopic the source topic
     */
    public void setSourceTopic(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    /**
     * Returns the target topic captured for the execution.
     *
     * @return the target topic
     */
    public String getTargetTopic() {
        return targetTopic;
    }

    /**
     * Sets the target topic captured for the execution.
     *
     * @param targetTopic the target topic
     */
    public void setTargetTopic(String targetTopic) {
        this.targetTopic = targetTopic;
    }

    /**
     * Returns the logical message-handler type used for validation.
     *
     * @return the message-handler type
     */
    public String getMessageType() {
        return messageType;
    }

    /**
     * Sets the logical message-handler type used for validation.
     *
     * @param messageType the message-handler type
     */
    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    /**
     * Returns the terminal error message, when present.
     *
     * @return the error message
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Sets the terminal error message.
     *
     * @param errorMessage the error message
     */
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    /**
     * Returns the number of committed transactions recorded for the execution.
     *
     * @return the committed transaction count
     */
    public Integer getCommittedTransactions() {
        return committedTransactions;
    }

    /**
     * Sets the number of committed transactions recorded for the execution.
     *
     * @param committedTransactions the committed transaction count
     */
    public void setCommittedTransactions(Integer committedTransactions) {
        this.committedTransactions = committedTransactions;
    }

    /**
     * Returns the number of restored records recorded for the execution.
     *
     * @return the restored record count
     */
    public Long getRecordsRestored() {
        return recordsRestored;
    }

    /**
     * Sets the number of restored records recorded for the execution.
     *
     * @param recordsRestored the restored record count
     */
    public void setRecordsRestored(Long recordsRestored) {
        this.recordsRestored = recordsRestored;
    }

    @PrePersist
    /**
     * Initializes the update timestamp before the entity is first persisted.
     */
    void onCreate() {
        updatedAt = Instant.now();
    }

    @PreUpdate
    /**
     * Refreshes the update timestamp before the entity is updated.
     */
    void onUpdate() {
        updatedAt = Instant.now();
    }
}
