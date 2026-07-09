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

@RestController
@RequestMapping("/api")
public class RestoreJobController {

    private final RestoreJobCoordinator restoreJobCoordinator;
    private final KafkaMessagePreviewService previewService;

    public RestoreJobController(RestoreJobCoordinator restoreJobCoordinator, KafkaMessagePreviewService previewService) {
        this.restoreJobCoordinator = restoreJobCoordinator;
        this.previewService = previewService;
    }

    @PostMapping("/restores")
    public ResponseEntity<RestoreJobStartResponse> startRestore(@Valid @RequestBody RestoreRequest request) {
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(restoreJobCoordinator.startJob(request.getRestoreType(), request.getRestoreFromTimestamp()));
    }

    @GetMapping("/jobs")
    public ResponseEntity<List<RestoreJobResponse>> listJobs() {
        return ResponseEntity.ok(restoreJobCoordinator.listJobs());
    }

    @GetMapping("/jobs/{jobId}")
    public ResponseEntity<RestoreJobResponse> getJob(@PathVariable UUID jobId) {
        return ResponseEntity.ok(restoreJobCoordinator.getJob(jobId));
    }

    @PostMapping("/jobs/{jobId}/cancel")
    public ResponseEntity<RestoreJobResponse> cancelJob(@PathVariable UUID jobId) {
        return ResponseEntity.ok(restoreJobCoordinator.requestCancellation(jobId));
    }

    @GetMapping("/pipelines/{restoreType}/messages")
    public ResponseEntity<KafkaMessagePreviewResponse> previewMessages(
            @PathVariable String restoreType,
            @RequestParam(defaultValue = "source") String topic,
            @RequestParam(defaultValue = "10") int limit
    ) {
        return ResponseEntity.ok(previewService.preview(restoreType, topic, limit));
    }
}
