package com.taskmesh.api.service;

import com.taskmesh.api.dto.CreateJobRequest;
import com.taskmesh.api.dto.JobResponse;
import com.taskmesh.api.kafka.JobEventPublisher;
import com.taskmesh.api.repository.JobRepository;
import com.taskmesh.common.entity.Job;
import com.taskmesh.common.enums.JobStatus;
import com.taskmesh.common.event.JobCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class JobService {

    private final JobRepository jobRepository;
    private final JobEventPublisher eventPublisher;

    /**
     * Create a new job with idempotency support.
     * If a job with the same idempotency key already exists, returns the existing job
     * without creating a duplicate — critical for AI agents that may retry requests.
     */
    @Transactional
    public JobResponse createJob(CreateJobRequest request, String idempotencyKey) {
        // Check idempotency — if key exists, return the cached result
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            Optional<Job> existing = jobRepository.findByIdempotencyKey(idempotencyKey);
            if (existing.isPresent()) {
                log.info("Idempotency hit for key={} returning jobId={}", idempotencyKey, existing.get().getId());
                return mapToResponse(existing.get());
            }
        }

        String traceId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);

        Instant scheduledAt = null;
        if (request.getScheduledAt() != null) {
            scheduledAt = Instant.parse(request.getScheduledAt());
        }

        Job job = Job.builder()
            .type(request.getType())
            .status(JobStatus.QUEUED)
            .priority(request.getPriority())
            .payload(request.getPayload())
            .requiredCapabilities(request.getRequiredCapabilities())
            .maxRetries(request.getMaxRetries())
            .timeoutSeconds(request.getTimeoutSeconds())
            .scheduledAt(scheduledAt)
            .cronExpression(request.getCronExpression())
            .idempotencyKey(idempotencyKey)
            .traceId(traceId)
            .build();

        job = jobRepository.save(job);
        log.info("Job created jobId={} type={} priority={} traceId={}",
            job.getId(), job.getType(), job.getPriority(), traceId);

        // Publish to Kafka so the scheduler picks it up asynchronously
        JobCreatedEvent event = new JobCreatedEvent(
            job.getId(), job.getType(), job.getPriority(),
            job.getPayload(), job.getRequiredCapabilities(),
            job.getMaxRetries(), job.getTimeoutSeconds(),
            job.getScheduledAt(), idempotencyKey, traceId, job.getCreatedAt()
        );
        eventPublisher.publishJobCreated(event);

        return mapToResponse(job);
    }

    @Transactional(readOnly = true)
    public Optional<JobResponse> getJob(UUID jobId) {
        return jobRepository.findById(jobId).map(this::mapToResponse);
    }

    @Transactional(readOnly = true)
    public List<JobResponse> listJobs(JobStatus status) {
        List<Job> jobs = (status != null)
            ? jobRepository.findByStatusOrderByCreatedAtDesc(status)
            : jobRepository.findAllByOrderByCreatedAtDesc();
        return jobs.stream().map(this::mapToResponse).collect(Collectors.toList());
    }

    /**
     * Cancel a job. If QUEUED/SCHEDULED/ASSIGNED → immediately CANCELLED.
     * If RUNNING → mark CANCELLED (worker checks DB before/during execution).
     */
    @Transactional
    public Optional<JobResponse> cancelJob(UUID jobId) {
        return jobRepository.findById(jobId).map(job -> {
            if (job.getStatus() == JobStatus.COMPLETED || job.getStatus() == JobStatus.DEAD_LETTER) {
                throw new IllegalStateException("Cannot cancel job in status: " + job.getStatus());
            }
            job.setStatus(JobStatus.CANCELLED);
            job = jobRepository.save(job);
            log.info("Job CANCELLED jobId={}", jobId);
            return mapToResponse(job);
        });
    }

    /**
     * Force retry a FAILED job — re-queues it and publishes to Kafka.
     */
    @Transactional
    public Optional<JobResponse> retryJob(UUID jobId) {
        return jobRepository.findById(jobId).map(job -> {
            if (job.getStatus() != JobStatus.FAILED && job.getStatus() != JobStatus.DEAD_LETTER) {
                throw new IllegalStateException("Can only retry FAILED or DEAD_LETTER jobs");
            }
            job.setStatus(JobStatus.QUEUED);
            job.setAssignedWorkerId(null);
            job.setErrorMessage(null);
            job = jobRepository.save(job);
            log.info("Job RETRYING jobId={} attempt={}/{}", jobId, job.getAttemptCount(), job.getMaxRetries());

            JobCreatedEvent event = new JobCreatedEvent(
                job.getId(), job.getType(), job.getPriority(),
                job.getPayload(), job.getRequiredCapabilities(),
                job.getMaxRetries(), job.getTimeoutSeconds(),
                null, null, job.getTraceId(), Instant.now()
            );
            eventPublisher.publishJobCreated(event);
            return mapToResponse(job);
        });
    }

    public JobResponse mapToResponse(Job job) {
        return JobResponse.builder()
            .jobId(job.getId())
            .type(job.getType())
            .status(job.getStatus())
            .priority(job.getPriority())
            .payload(job.getPayload())
            .requiredCapabilities(job.getRequiredCapabilities())
            .maxRetries(job.getMaxRetries())
            .attemptCount(job.getAttemptCount())
            .timeoutSeconds(job.getTimeoutSeconds())
            .assignedWorkerId(job.getAssignedWorkerId())
            .result(job.getResult())
            .errorMessage(job.getErrorMessage())
            .traceId(job.getTraceId())
            .workflowId(job.getWorkflowId())
            .workflowNodeKey(job.getWorkflowNodeKey())
            .createdAt(job.getCreatedAt())
            .updatedAt(job.getUpdatedAt())
            .build();
    }
}
