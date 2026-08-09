package com.taskmesh.api.dto;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Data
@Builder
public class WorkerResponse {
    private String workerId;
    private String capabilities;
    private int maxConcurrency;
    private int activeJobs;
    private String status;
    private double loadPercent;
    private Instant lastHeartbeat;
    private Instant registeredAt;
}
