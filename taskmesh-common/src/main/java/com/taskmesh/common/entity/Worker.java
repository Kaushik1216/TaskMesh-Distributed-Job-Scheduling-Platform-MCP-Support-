package com.taskmesh.common.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

/**
 * Worker node registration entity.
 * Workers register themselves on startup and maintain liveness via heartbeats.
 * The scheduler uses this to select capable workers for job assignment.
 */
@Entity
@Table(name = "workers")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Worker {

    @Id
    @Column(name = "worker_id")
    private String workerId;

    /** Comma-separated list of job types this worker can handle */
    @Column(columnDefinition = "VARCHAR(500)")
    private String capabilities;

    @Column(name = "max_concurrency", nullable = false)
    @Builder.Default
    private int maxConcurrency = 4;

    @Column(name = "active_jobs", nullable = false)
    @Builder.Default
    private int activeJobs = 0;

    @Column(nullable = false)
    @Builder.Default
    private String status = "ACTIVE";

    @Column(name = "last_heartbeat")
    private Instant lastHeartbeat;

    @Column(name = "registered_at", nullable = false, updatable = false)
    @Builder.Default
    private Instant registeredAt = Instant.now();
}
