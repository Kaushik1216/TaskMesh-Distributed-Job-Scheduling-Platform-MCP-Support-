package com.taskmesh.worker.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskmesh.common.entity.Job;
import com.taskmesh.common.enums.JobStatus;
import com.taskmesh.common.enums.JobType;
import com.taskmesh.common.event.JobCompletedEvent;
import com.taskmesh.common.event.JobFailedEvent;
import com.taskmesh.worker.handler.JobHandler;
import com.taskmesh.worker.repository.JobRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Concurrent job executor using a bounded thread pool and Semaphore.
 *
 * Key distributed systems / concurrency concepts demonstrated:
 *
 * 1. BOUNDED CONCURRENCY:
 *    A Semaphore(maxConcurrency) enforces the worker's declared concurrency limit.
 *    If the semaphore is exhausted, the job is rejected and re-queued (not blocked).
 *
 * 2. HANDLER REGISTRY:
 *    On startup, all JobHandler beans are auto-wired and registered in a Map<JobType, JobHandler>.
 *    New job types can be added by just creating a new @Component implementing JobHandler.
 *
 * 3. TIMEOUT ENFORCEMENT:
 *    Each job runs in a Future. Future.get(timeoutSec) enforces the hard timeout.
 *    Timed-out jobs trigger the retry/DLQ path.
 *
 * 4. GRACEFUL SHUTDOWN:
 *    @PreDestroy waits up to 30s for in-flight jobs to complete before force-stopping.
 *
 * 5. STATUS VISIBILITY:
 *    Job DB status is updated to RUNNING before execution and COMPLETED/FAILED after.
 *    This allows the API to show real-time job progress.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ConcurrentJobExecutor {

    private static final String TOPIC_COMPLETED = "taskmesh.jobs.completed";
    private static final String TOPIC_FAILED    = "taskmesh.jobs.failed";

    @Value("${taskmesh.worker.id:worker-1}")
    private String workerId;

    @Value("${taskmesh.worker.max-concurrency:4}")
    private int maxConcurrency;

    private final List<JobHandler>                   handlers;
    private final JobRepository                      jobRepository;
    private final KafkaTemplate<String, String>      kafkaTemplate;
    private final ObjectMapper                       objectMapper;

    private Map<JobType, JobHandler>  handlerRegistry;
    private ThreadPoolExecutor        executorService;
    private Semaphore                 concurrencyLimiter;
    private final ConcurrentHashMap<UUID, Future<?>> activeFutures = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        handlerRegistry = handlers.stream()
            .collect(Collectors.toMap(JobHandler::getSupportedType, Function.identity()));
        log.info("Worker [{}] loaded {} handlers: {}", workerId, handlerRegistry.size(), handlerRegistry.keySet());

        // Fixed thread pool — each slot maps to one concurrent job
        executorService = new ThreadPoolExecutor(
            maxConcurrency, maxConcurrency,
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(maxConcurrency * 2),
            r -> new Thread(r, workerId + "-exec-" + System.nanoTime()),
            new ThreadPoolExecutor.CallerRunsPolicy()
        );

        concurrencyLimiter = new Semaphore(maxConcurrency, true);
        log.info("Worker [{}] ready — maxConcurrency={}", workerId, maxConcurrency);
    }

    /**
     * Submit a job for async execution. Returns immediately.
     * If at capacity, rejects with a failed event so the scheduler can reassign.
     */
    public void submitJob(Job job) {
        if (!concurrencyLimiter.tryAcquire()) {
            log.warn("[{}] At capacity ({}/{}) — rejecting jobId={}",
                workerId, maxConcurrency, maxConcurrency, job.getId());
            // Fail immediately so scheduler can pick another worker
            publishFailed(job, "Worker at max concurrency — rejected", false);
            return;
        }

        Future<?> future = executorService.submit(() -> executeJob(job));
        activeFutures.put(job.getId(), future);
        log.info("[{}] Accepted jobId={} type={} | activeJobs={}/{}",
            workerId, job.getId(), job.getType(),
            maxConcurrency - concurrencyLimiter.availablePermits(), maxConcurrency);
    }

    private void executeJob(Job job) {
        long startMs = System.currentTimeMillis();
        try {
            // Mark RUNNING in DB — visible via GET /api/v1/jobs/{id}
            job.setStatus(JobStatus.RUNNING);
            jobRepository.save(job);
            log.info("[{}] RUNNING jobId={} type={} attempt={}/{}",
                workerId, job.getId(), job.getType(), job.getAttemptCount() + 1, job.getMaxRetries());

            // Check cancellation before doing any work
            Job latest = jobRepository.findById(job.getId()).orElse(job);
            if (latest.getStatus() == JobStatus.CANCELLED) {
                log.info("[{}] Job {} was cancelled — aborting", workerId, job.getId());
                return;
            }

            // Dispatch to the right handler
            JobHandler handler = handlerRegistry.get(job.getType());
            if (handler == null) {
                throw new IllegalArgumentException("No handler registered for type: " + job.getType());
            }

            // Execute with timeout using a separate Future
            CompletableFuture<String> execFuture = CompletableFuture.supplyAsync(
                () -> {
                    try { return handler.execute(job.getPayload(), job.getTimeoutSeconds()); }
                    catch (Exception e) { throw new RuntimeException(e); }
                }, executorService);

            String result = execFuture.get(job.getTimeoutSeconds(), TimeUnit.SECONDS);
            long durationMs = System.currentTimeMillis() - startMs;

            // Persist result
            job.setStatus(JobStatus.COMPLETED);
            job.setResult(result);
            job.setAttemptCount(job.getAttemptCount() + 1);
            jobRepository.save(job);

            // Notify scheduler of completion
            JobCompletedEvent event = new JobCompletedEvent(
                job.getId(), workerId, result, durationMs, job.getTraceId(), Instant.now());
            publishEvent(TOPIC_COMPLETED, job.getId().toString(), event);
            log.info("[{}] COMPLETED jobId={} type={} in {}ms", workerId, job.getId(), job.getType(), durationMs);

        } catch (TimeoutException e) {
            log.error("[{}] TIMEOUT jobId={} after {}s", workerId, job.getId(), job.getTimeoutSeconds());
            job.setAttemptCount(job.getAttemptCount() + 1);
            jobRepository.save(job);
            publishFailed(job, "Execution timed out after " + job.getTimeoutSeconds() + "s", true);

        } catch (CancellationException | InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[{}] INTERRUPTED jobId={}", workerId, job.getId());
            publishFailed(job, "Execution interrupted", true);

        } catch (Exception e) {
            log.error("[{}] FAILED jobId={} type={} attempt={}/{}: {}",
                workerId, job.getId(), job.getType(), job.getAttemptCount() + 1, job.getMaxRetries(), e.getMessage());
            job.setAttemptCount(job.getAttemptCount() + 1);
            jobRepository.save(job);
            publishFailed(job, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName(), true);

        } finally {
            activeFutures.remove(job.getId());
            concurrencyLimiter.release();
        }
    }

    private void publishFailed(Job job, String errorMessage, boolean checkRetry) {
        boolean willRetry = checkRetry && job.getAttemptCount() < job.getMaxRetries();
        JobFailedEvent event = new JobFailedEvent(
            job.getId(), workerId, errorMessage,
            job.getAttemptCount(), job.getMaxRetries(),
            willRetry, job.getTraceId(), Instant.now());
        publishEvent(TOPIC_FAILED, job.getId().toString(), event);
    }

    private void publishEvent(String topic, String key, Object event) {
        try {
            kafkaTemplate.send(topic, key, objectMapper.writeValueAsString(event));
        } catch (Exception e) {
            log.error("Failed to publish to topic={} key={}", topic, key, e);
        }
    }

    public int getActiveJobCount() {
        return maxConcurrency - concurrencyLimiter.availablePermits();
    }

    @PreDestroy
    public void shutdown() {
        log.info("[{}] Shutting down — waiting for {} in-flight jobs", workerId, activeFutures.size());
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("[{}] Forcing shutdown after 30s grace period", workerId);
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
