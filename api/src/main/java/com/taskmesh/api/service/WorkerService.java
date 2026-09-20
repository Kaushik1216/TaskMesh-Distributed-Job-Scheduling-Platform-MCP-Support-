package com.taskmesh.api.service;

import com.taskmesh.api.dto.WorkerRegistrationRequest;
import com.taskmesh.api.dto.WorkerResponse;
import com.taskmesh.api.repository.WorkerRepository;
import com.taskmesh.common.entity.Worker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class WorkerService {

    private final WorkerRepository workerRepository;

    /** Upsert worker registration — idempotent (workers call this on every startup). */
    @Transactional
    public WorkerResponse registerOrUpdate(WorkerRegistrationRequest request) {
        Worker worker = workerRepository.findById(request.getWorkerId())
            .orElse(Worker.builder()
                .workerId(request.getWorkerId())
                .build());

        worker.setCapabilities(request.getCapabilities());
        worker.setMaxConcurrency(request.getMaxConcurrency());
        worker.setStatus("ACTIVE");
        worker.setLastHeartbeat(Instant.now());
        worker = workerRepository.save(worker);

        log.info("Worker registered/updated workerId={} caps={} maxConcurrency={}",
            worker.getWorkerId(), worker.getCapabilities(), worker.getMaxConcurrency());
        return mapToResponse(worker);
    }

    /** Update last heartbeat timestamp for a given worker. */
    @Transactional
    public Optional<WorkerResponse> heartbeat(String workerId) {
        return workerRepository.findById(workerId).map(w -> {
            w.setLastHeartbeat(Instant.now());
            if ("DEAD".equals(w.getStatus())) {
                w.setStatus("ACTIVE");
                log.info("Worker resurrected from DEAD: {}", workerId);
            }
            return mapToResponse(workerRepository.save(w));
        });
    }

    @Transactional(readOnly = true)
    public List<WorkerResponse> getAllWorkers() {
        return workerRepository.findAll().stream()
            .map(this::mapToResponse)
            .collect(Collectors.toList());
    }

    @Transactional(readOnly = true)
    public Optional<WorkerResponse> getWorker(String workerId) {
        return workerRepository.findById(workerId).map(this::mapToResponse);
    }

    /**
     * Mark workers as DEAD if heartbeat hasn't been received in 30 seconds.
     * Run every 15 seconds. Dead workers are excluded from job scheduling.
     */
    @Scheduled(fixedDelay = 15_000)
    @Transactional
    public void markDeadWorkers() {
        Instant threshold = Instant.now().minusSeconds(30);
        List<Worker> dead = workerRepository.findDeadWorkers(threshold);
        if (!dead.isEmpty()) {
            dead.forEach(w -> {
                w.setStatus("DEAD");
                w.setActiveJobs(0);
                log.warn("Worker marked DEAD (heartbeat timeout): {}", w.getWorkerId());
            });
            workerRepository.saveAll(dead);
        }
    }

    private WorkerResponse mapToResponse(Worker w) {
        double load = w.getMaxConcurrency() == 0 ? 0.0
            : (double) w.getActiveJobs() / w.getMaxConcurrency() * 100;
        return WorkerResponse.builder()
            .workerId(w.getWorkerId())
            .capabilities(w.getCapabilities())
            .maxConcurrency(w.getMaxConcurrency())
            .activeJobs(w.getActiveJobs())
            .status(w.getStatus())
            .loadPercent(Math.round(load * 10.0) / 10.0)
            .lastHeartbeat(w.getLastHeartbeat())
            .registeredAt(w.getRegisteredAt())
            .build();
    }
}
