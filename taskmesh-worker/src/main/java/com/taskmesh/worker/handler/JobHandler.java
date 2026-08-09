package com.taskmesh.worker.handler;

import com.taskmesh.common.enums.JobType;

/**
 * Strategy interface for executing specific job types.
 * Each implementation handles one JobType and simulates the actual work.
 * In production, these would shell out to real tools (test runners, build systems, etc.)
 */
public interface JobHandler {

    /**
     * Execute the job and return a JSON result string.
     * @param payload   job-specific configuration JSON
     * @param timeoutSec maximum allowed execution time
     * @return result JSON string
     * @throws Exception on execution failure (causes retry logic to trigger)
     */
    String execute(String payload, int timeoutSec) throws Exception;

    JobType getSupportedType();
}
