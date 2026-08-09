package com.taskmesh.scheduler.strategy;

import com.taskmesh.common.entity.Job;
import com.taskmesh.common.entity.Worker;

import java.util.List;
import java.util.Optional;

/**
 * Pluggable scheduling strategy interface.
 * Different implementations allow swapping the scheduling algorithm
 * without changing the scheduler service.
 */
public interface SchedulingStrategy {
    /**
     * Select the best worker for the given job.
     * @param job the job to schedule
     * @param availableWorkers list of ACTIVE workers with remaining capacity
     * @return the chosen worker, or empty if none is suitable
     */
    Optional<Worker> selectWorker(Job job, List<Worker> availableWorkers);

    String getName();
}
