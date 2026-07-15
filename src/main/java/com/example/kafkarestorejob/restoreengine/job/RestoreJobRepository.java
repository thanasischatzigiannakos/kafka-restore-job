package com.example.kafkarestorejob.restoreengine.job;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface RestoreJobRepository extends JpaRepository<RestoreJobEntity, UUID> {

    List<RestoreJobEntity> findTop50ByOrderByRequestedAtDesc();

    @Modifying
    @Query("""
            update RestoreJobEntity entity
            set entity.status = :newStatus,
                entity.cancellationRequestedAt = :cancellationRequestedAt
            where entity.id = :jobId
              and entity.status in :currentStatuses
            """)
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
    int updateCompleted(
            @Param("jobId") UUID jobId,
            @Param("newStatus") RestoreJobStatus newStatus,
            @Param("completedAt") java.time.Instant completedAt,
            @Param("currentStatus") RestoreJobStatus currentStatus
    );
}
