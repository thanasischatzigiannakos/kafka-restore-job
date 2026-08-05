package com.example.kafkarestorejob.restoreengine.api;

import com.example.kafkarestorejob.restoreengine.job.RestoreJobCoordinator;
import com.example.kafkarestorejob.restoreengine.job.RestoreJobResponse;
import com.example.kafkarestorejob.restoreengine.job.RestoreJobStartResponse;
import com.example.kafkarestorejob.restoreengine.service.KafkaMessagePreviewService;
import jakarta.validation.Valid;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller exposing restore execution and topic-preview endpoints.
 */
@RestController
@RequestMapping("/api")
public class RestoreJobController {

    private final RestoreJobCoordinator restoreJobCoordinator;
    private final KafkaMessagePreviewService previewService;

    /**
     * Creates the controller with the collaborators that coordinate restore jobs and preview topic
     * messages.
     *
     * @param restoreJobCoordinator the restore job coordinator
     * @param previewService the preview service for source and target topics
     */
    public RestoreJobController(RestoreJobCoordinator restoreJobCoordinator, KafkaMessagePreviewService previewService) {
        this.restoreJobCoordinator = restoreJobCoordinator;
        this.previewService = previewService;
    }

    @PostMapping("/restores")
    /**
     * Starts a new restore job asynchronously.
     *
     * @param request the restore request payload
     * @return the accepted restore job identifier
     */
    public ResponseEntity<RestoreJobStartResponse> startRestore(@Valid @RequestBody RestoreRequest request) {
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(restoreJobCoordinator.startJob(request.getRestoreType(), request.getRestoreFromTimestamp()));
    }

    @GetMapping("/jobs")
    /**
     * Lists the most recent restore jobs.
     *
     * @return the restore job summaries
     */
    public ResponseEntity<List<RestoreJobResponse>> listJobs() {
        return ResponseEntity.ok(restoreJobCoordinator.listJobs());
    }

    @GetMapping("/jobs/{jobId}")
    /**
     * Returns the current state of one restore job.
     *
     * @param jobId the restore job identifier
     * @return the restore job summary
     */
    public ResponseEntity<RestoreJobResponse> getJob(@PathVariable UUID jobId) {
        return ResponseEntity.ok(restoreJobCoordinator.getJob(jobId));
    }

    @PostMapping("/jobs/{jobId}/cancel")
    /**
     * Requests cancellation of a pending or running restore job.
     *
     * @param jobId the restore job identifier
     * @return the updated restore job summary
     */
    public ResponseEntity<RestoreJobResponse> cancelJob(@PathVariable UUID jobId) {
        return ResponseEntity.ok(restoreJobCoordinator.requestCancellation(jobId));
    }

    @GetMapping("/pipelines/{restoreType}/messages")
    /**
     * Returns a bounded preview of records from either the source or the target topic of a
     * configured pipeline.
     *
     * @param restoreType the logical restore type whose pipeline should be previewed
     * @param topic the topic selector, typically {@code source} or {@code target}
     * @param limit the maximum number of records to preview
     * @return the preview response
     */
    public ResponseEntity<KafkaMessagePreviewResponse> previewMessages(
            @PathVariable String restoreType,
            @RequestParam(defaultValue = "source") String topic,
            @RequestParam(defaultValue = "10") int limit
    ) {
        return ResponseEntity.ok(previewService.preview(restoreType, topic, limit));
    }
}
