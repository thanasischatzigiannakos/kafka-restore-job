package com.example.kafkarestorejob.restoreengine.job;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RestoreJobRepository extends JpaRepository<RestoreJobEntity, UUID> {

    boolean existsByRestoreTypeAndStatusIn(String restoreType, Collection<RestoreJobStatus> statuses);

    List<RestoreJobEntity> findTop50ByOrderByRequestedAtDesc();
}
