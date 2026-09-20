package com.taskmesh.api.controller;

import com.taskmesh.api.dto.CreateJobRequest;
import com.taskmesh.api.dto.JobResponse;
import com.taskmesh.api.service.JobService;
import com.taskmesh.common.enums.JobStatus;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

/**
 * REST API for job lifecycle management.
 *
 * Key design: POST /api/v1/jobs accepts an optional Idempotency-Key header.
 * AI agents that retry failed requests won't create duplicate jobs.
 */
@RestController
@RequestMapping("/api/v1/jobs")
@RequiredArgsConstructor
@Slf4j
public class JobController {

    private final JobService jobService;

    /**
     * Submit a new job. Supports idempotency via the Idempotency-Key header.
     * Example: curl -X POST /api/v1/jobs -H "Idempotency-Key: my-unique-key" -d '{...}'
     */
    @PostMapping
    public ResponseEntity<JobResponse> createJob(
            @Valid @RequestBody CreateJobRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {
        log.info("POST /api/v1/jobs type={} priority={} idempotencyKey={}",
            request.getType(), request.getPriority(), idempotencyKey);
        JobResponse response = jobService.createJob(request, idempotencyKey);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<JobResponse> getJob(@PathVariable(name = "jobId") UUID jobId) {
        return jobService.getJob(jobId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public ResponseEntity<List<JobResponse>> listJobs(
            @RequestParam(name = "status", required = false) JobStatus status) {
        return ResponseEntity.ok(jobService.listJobs(status));
    }

    @DeleteMapping("/{jobId}")
    public ResponseEntity<JobResponse> cancelJob(@PathVariable(name = "jobId") UUID jobId) {
        return jobService.cancelJob(jobId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{jobId}/retry")
    public ResponseEntity<JobResponse> retryJob(@PathVariable(name = "jobId") UUID jobId) {
        return jobService.retryJob(jobId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }
}
