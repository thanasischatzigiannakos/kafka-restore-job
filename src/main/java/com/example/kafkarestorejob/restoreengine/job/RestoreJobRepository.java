package com.example.kafkarestorejob.restoreengine.job;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RestoreJobRepository extends JpaRepository<RestoreJobEntity, UUID> {

    List<RestoreJobEntity> findTop50ByOrderByRequestedAtDesc();
}
