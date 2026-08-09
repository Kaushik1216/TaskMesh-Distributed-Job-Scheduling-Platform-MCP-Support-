package com.taskmesh.scheduler.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskmesh.common.entity.Job;
import com.taskmesh.common.entity.Worker;
import com.taskmesh.common.enums.JobStatus;
import com.taskmesh.common.event.JobAssignedEvent;
import com.taskmesh.common.event.JobCompletedEvent;
import com.taskmesh.common.event.JobCreatedEvent;
import com.taskmesh.common.event.JobFailedEvent;
import com.taskmesh.scheduler.repository.JobRepository;
import com.taskmesh.scheduler.repository.WorkerRepository;
import com.taskmesh.scheduler.strategy.CapabilityAwareStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Core distributed scheduling engine.
 *
 * Only the LEADER instance actively assigns jobs.
 * Standby instances remain idle, ready to take over if the leader fails.
 *
 * Job assignment flow:
 *   1. Listen on taskmesh.jobs.created
 *   2. Find best worker using CapabilityAwareStrategy
 *   3. Increment worker.activeJobs (optimistic load tracking)
 *   4. Publish JobAssignedEvent to taskmesh.jobs.assigned
 *   5. Worker receives assignment and starts execution
 *
 * Retry flow (exponential backoff):
 *   failure → RETRYING (scheduledAt = now + 2^attempt * 2s) → re-queued by periodic scan
 *   max retries exceeded → DEAD_LETTER → published to dead-letter topic
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class JobSchedulerService {

    private static final String TOPIC_ASSIGNED    = "taskmesh.jobs.assigned";
    private static final String TOPIC_DEAD_LETTER = "taskmesh.jobs.dead-letter";
    private static final long   BASE_RETRY_MS     = 2_000L;

    private final JobRepository          jobRepository;
    private final WorkerRepository       workerRepository;
    private final LeaderElectionService  leaderElection;
    private final CapabilityAwareStrategy strategy;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper           objectMapper;

    // ─── Kafka Listeners ────────────────────────────────────────────────────

    @KafkaListener(topics = "taskmesh.jobs.created", groupId = "taskmesh-scheduler")
    @Transactional
    public void onJobCreated(ConsumerRecord<String, String> record) {
        if (!leaderElection.isLeader()) return; // standby — ignore

        try {
            JobCreatedEvent event = objectMapper.readValue(record.value(), JobCreatedEvent.class);
            log.info("[LEADER:{}] Received job jobId={} type={} priority={}",
                leaderElection.getInstanceId(), event.jobId(), event.type(), event.priority());
            assignJobToWorker(event.jobId());
        } catch (Exception e) {
            log.error("Error handling JobCreatedEvent", e);
        }
    }

    @KafkaListener(topics = "taskmesh.jobs.completed", groupId = "taskmesh-scheduler")
    @Transactional
    public void onJobCompleted(ConsumerRecord<String, String> record) {
        try {
            JobCompletedEvent event = objectMapper.readValue(record.value(), JobCompletedEvent.class);
            jobRepository.findById(event.jobId()).ifPresent(job -> {
                job.setStatus(JobStatus.COMPLETED);
                job.setResult(event.result());
                jobRepository.save(job);
                decrementWorkerLoad(event.workerId());
                log.info("Job COMPLETED jobId={} workerId={} durationMs={}",
                    event.jobId(), event.workerId(), event.durationMs());
            });
        } catch (Exception e) {
            log.error("Error handling JobCompletedEvent", e);
        }
    }

    @KafkaListener(topics = "taskmesh.jobs.failed", groupId = "taskmesh-scheduler")
    @Transactional
    public void onJobFailed(ConsumerRecord<String, String> record) {
        try {
            JobFailedEvent event = objectMapper.readValue(record.value(), JobFailedEvent.class);
            jobRepository.findById(event.jobId()).ifPresent(job -> {
                decrementWorkerLoad(job.getAssignedWorkerId());
                job.setAttemptCount(event.attemptCount());
                job.setErrorMessage(event.errorMessage());

                if (event.attemptCount() >= event.maxRetries()) {
                    // Exhausted retries — move to dead-letter queue
                    job.setStatus(JobStatus.DEAD_LETTER);
                    job.setAssignedWorkerId(null);
                    jobRepository.save(job);
                    publishToDeadLetter(job);
                    log.warn("Job DEAD_LETTER jobId={} after {}/{} attempts",
                        event.jobId(), event.attemptCount(), event.maxRetries());
                } else {
                    // Exponential backoff: delay = 2^attempt * BASE_RETRY_MS
                    long delayMs = BASE_RETRY_MS * (1L << event.attemptCount());
                    job.setStatus(JobStatus.RETRYING);
                    job.setScheduledAt(Instant.now().plusMillis(delayMs));
                    job.setAssignedWorkerId(null);
                    jobRepository.save(job);
                    log.info("Job RETRYING jobId={} attempt={}/{} delay={}ms",
                        event.jobId(), event.attemptCount(), event.maxRetries(), delayMs);
                }
            });
        } catch (Exception e) {
            log.error("Error handling JobFailedEvent", e);
        }
    }

    // ─── Periodic Job Scan ───────────────────────────────────────────────────

    /**
     * Periodic fallback scan — picks up QUEUED/RETRYING jobs that might have been
     * missed (e.g., scheduler was standby when the Kafka event arrived).
     * Also processes retry-due jobs whose scheduledAt has passed.
     * Sorts by priority (CRITICAL first) then creation time (FIFO within same priority).
     */
    @Scheduled(fixedDelay = 10_000)
    @Transactional
    public void processQueuedJobs() {
        if (!leaderElection.isLeader()) return;

        List<Job> toProcess = new ArrayList<>();

        // QUEUED jobs (priority-sorted)
        List<Job> queued = jobRepository.findByStatusOrderByCreatedAtAsc(JobStatus.QUEUED);
        queued.sort(Comparator.comparingInt((Job j) -> j.getPriority().getLevel()).reversed()
            .thenComparing(Job::getCreatedAt));
        toProcess.addAll(queued);

        // RETRYING jobs whose delay has expired
        List<Job> retrying = jobRepository.findByStatusOrderByCreatedAtAsc(JobStatus.RETRYING)
            .stream()
            .filter(j -> j.getScheduledAt() == null || !j.getScheduledAt().isAfter(Instant.now()))
            .toList();
        toProcess.addAll(retrying);

        if (!toProcess.isEmpty()) {
            log.info("[LEADER] Periodic scan: processing {} queued/retry jobs", toProcess.size());
            toProcess.forEach(j -> assignJobToWorker(j.getId()));
        }
    }

    // ─── Core Assignment Logic ───────────────────────────────────────────────

    @Transactional
    public void assignJobToWorker(UUID jobId) {
        Optional<Job> jobOpt = jobRepository.findById(jobId);
        if (jobOpt.isEmpty()) return;

        Job job = jobOpt.get();
        if (job.getStatus() == JobStatus.CANCELLED || job.getStatus() == JobStatus.COMPLETED) return;

        List<Worker> activeWorkers = workerRepository.findByStatus("ACTIVE");
        if (activeWorkers.isEmpty()) {
            log.warn("No ACTIVE workers available for jobId={}", jobId);
            return;
        }

        Optional<Worker> chosen = strategy.selectWorker(job, activeWorkers);
        if (chosen.isEmpty()) {
            log.warn("No suitable worker for jobId={} caps={}", jobId, job.getRequiredCapabilities());
            return;
        }

        Worker worker = chosen.get();

        // Update job state
        job.setStatus(JobStatus.ASSIGNED);
        job.setAssignedWorkerId(worker.getWorkerId());
        jobRepository.save(job);

        // Optimistically increment worker load
        worker.setActiveJobs(worker.getActiveJobs() + 1);
        workerRepository.save(worker);

        // Publish assignment event so the specific worker picks it up
        JobAssignedEvent event = new JobAssignedEvent(
            job.getId(), worker.getWorkerId(), job.getTraceId(), Instant.now());
        publishEvent(TOPIC_ASSIGNED, jobId.toString(), event);

        log.info("[LEADER] ASSIGNED jobId={} → workerId={} load={}/{} strategy={}",
            jobId, worker.getWorkerId(), worker.getActiveJobs(),
            worker.getMaxConcurrency(), strategy.getName());
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private void decrementWorkerLoad(String workerId) {
        if (workerId == null) return;
        workerRepository.findById(workerId).ifPresent(w -> {
            w.setActiveJobs(Math.max(0, w.getActiveJobs() - 1));
            workerRepository.save(w);
        });
    }

    private void publishToDeadLetter(Job job) {
        try {
            kafkaTemplate.send(TOPIC_DEAD_LETTER, job.getId().toString(),
                objectMapper.writeValueAsString(job));
        } catch (Exception e) {
            log.error("Failed to publish dead-letter for jobId={}", job.getId(), e);
        }
    }

    private void publishEvent(String topic, String key, Object event) {
        try {
            kafkaTemplate.send(topic, key, objectMapper.writeValueAsString(event));
        } catch (Exception e) {
            log.error("Failed to publish event to topic={} key={}", topic, key, e);
        }
    }
}
