package com.example.kafkarestorejob.restoreengine.job;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * JPA repository for persisted restore job entities.
 */
public interface RestoreJobRepository extends JpaRepository<RestoreJobEntity, UUID> {

    /**
     * Returns the most recently requested restore jobs.
     *
     * @return the newest persisted restore jobs
     */
    List<RestoreJobEntity> findTop50ByOrderByRequestedAtDesc();

    @Modifying
    @Query("""
            update RestoreJobEntity entity
            set entity.status = :newStatus,
                entity.cancellationRequestedAt = :cancellationRequestedAt
            where entity.id = :jobId
              and entity.status in :currentStatuses
            """)
    /**
     * Updates a restore job to the cancellation-requested state when its current state allows it.
     *
     * @param jobId the restore job identifier
     * @param newStatus the new status to persist
     * @param cancellationRequestedAt the cancellation timestamp
     * @param currentStatuses the statuses from which cancellation is allowed
     * @return the number of updated rows
     */
    int updateCancellationRequested(
            @Param("jobId") UUID jobId,
            @Param("newStatus") RestoreJobStatus newStatus,
            @Param("cancellationRequestedAt") java.time.Instant cancellationRequestedAt,
            @Param("currentStatuses") List<RestoreJobStatus> currentStatuses
    );

    @Modifying
    @Query("""
            update RestoreJobEntity entity
            set entity.status = :newStatus
            where entity.id = :jobId
              and entity.status = :currentStatus
            """)
    /**
     * Updates a restore job status only when it currently matches the supplied status.
     *
     * @param jobId the restore job identifier
     * @param newStatus the new status to persist
     * @param currentStatus the expected current status
     * @return the number of updated rows
     */
    int updateStatusIfCurrent(
            @Param("jobId") UUID jobId,
            @Param("newStatus") RestoreJobStatus newStatus,
            @Param("currentStatus") RestoreJobStatus currentStatus
    );

    @Modifying
    @Query("""
            update RestoreJobEntity entity
            set entity.status = :newStatus,
                entity.completedAt = :completedAt
            where entity.id = :jobId
              and entity.status = :currentStatus
            """)
    /**
     * Marks a restore job as completed only when it is currently finalizing.
     *
     * @param jobId the restore job identifier
     * @param newStatus the completed status to persist
     * @param completedAt the completion timestamp
     * @param currentStatus the expected current status
     * @return the number of updated rows
     */
    int updateCompleted(
            @Param("jobId") UUID jobId,
            @Param("newStatus") RestoreJobStatus newStatus,
            @Param("completedAt") java.time.Instant completedAt,
            @Param("currentStatus") RestoreJobStatus currentStatus
    );
}
