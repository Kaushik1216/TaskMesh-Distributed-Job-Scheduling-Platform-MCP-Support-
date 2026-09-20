package com.taskmesh.scheduler.strategy;

import com.taskmesh.common.entity.Job;
import com.taskmesh.common.entity.Worker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Capability-Aware + Least-Loaded Scheduling Strategy.
 *
 * Algorithm (two-phase):
 *
 * Phase 1 — Filter by capability:
 *   Only consider workers that have ALL required capabilities for the job.
 *   e.g. Job needs TEST_EXECUTION → workers without it are excluded.
 *
 * Phase 2 — Least loaded among capable workers:
 *   Compute load ratio = activeJobs / maxConcurrency for each capable worker.
 *   Pick the worker with the lowest ratio (most headroom).
 *
 * Result: Jobs go to workers that CAN do them AND have the most capacity.
 * This prevents both capability mismatches and hot spots under load.
 */
@Component
@Slf4j
public class CapabilityAwareStrategy implements SchedulingStrategy {

    @Override
    public Optional<Worker> selectWorker(Job job, List<Worker> availableWorkers) {
        String requiredCaps = job.getRequiredCapabilities();

        // No capability requirement — use any worker with remaining capacity
        if (requiredCaps == null || requiredCaps.isBlank()) {
            return availableWorkers.stream()
                .filter(w -> w.getActiveJobs() < w.getMaxConcurrency())
                .min((a, b) -> Double.compare(loadRatio(a), loadRatio(b)));
        }

        Set<String> required = Arrays.stream(requiredCaps.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toSet());

        Optional<Worker> selected = availableWorkers.stream()
            .filter(w -> hasAllCapabilities(w, required))
            .filter(w -> w.getActiveJobs() < w.getMaxConcurrency())
            .min((a, b) -> Double.compare(loadRatio(a), loadRatio(b)));

        selected.ifPresent(w ->
            log.debug("Strategy={} selected worker={} load={}% for job={} requiredCaps={}",
                getName(), w.getWorkerId(),
                String.format("%.0f", loadRatio(w) * 100),
                job.getId(), required));

        if (selected.isEmpty()) {
            log.warn("No capable worker found for requiredCaps={} (available={})",
                required, availableWorkers.stream().map(Worker::getWorkerId).toList());
        }

        return selected;
    }

    private boolean hasAllCapabilities(Worker worker, Set<String> required) {
        if (worker.getCapabilities() == null || worker.getCapabilities().isBlank()) return false;
        Set<String> workerCaps = Arrays.stream(worker.getCapabilities().split(","))
            .map(String::trim)
            .collect(Collectors.toSet());
        return workerCaps.containsAll(required);
    }

    private double loadRatio(Worker w) {
        return w.getMaxConcurrency() == 0 ? 1.0 : (double) w.getActiveJobs() / w.getMaxConcurrency();
    }

    @Override
    public String getName() {
        return "CAPABILITY_AWARE_LEAST_LOADED";
    }
}
