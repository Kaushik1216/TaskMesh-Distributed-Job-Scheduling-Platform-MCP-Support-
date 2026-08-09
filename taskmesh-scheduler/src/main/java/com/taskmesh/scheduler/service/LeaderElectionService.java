package com.taskmesh.scheduler.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Distributed leader election using Redis SETNX pattern.
 *
 * Multiple scheduler instances compete for the same Redis key.
 * Only the holder of the key is the LEADER and will assign jobs.
 *
 * If the leader crashes:
 *   1. Redis TTL expires (no renewal)
 *   2. Another standby scheduler acquires the key
 *   3. New leader starts processing jobs
 *
 * This ensures at-most-one scheduler assigns jobs at any time,
 * preventing duplicate job assignments in a distributed setup.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class LeaderElectionService {

    private static final String LEADER_KEY = "taskmesh:scheduler:leader";
    private static final Duration LEADER_TTL = Duration.ofSeconds(15);

    private final StringRedisTemplate redisTemplate;
    private final AtomicBoolean isLeader = new AtomicBoolean(false);

    @Value("${taskmesh.scheduler.instance-id:scheduler-1}")
    private String instanceId;

    /**
     * Attempt to acquire or renew leadership every 5 seconds.
     * Uses atomic SET key value NX PX for safe distributed locking.
     */
    @Scheduled(fixedDelay = 5_000)
    public void attemptLeadership() {
        String currentLeader = redisTemplate.opsForValue().get(LEADER_KEY);

        if (instanceId.equals(currentLeader)) {
            // We are the leader — renew TTL to prevent expiry
            redisTemplate.expire(LEADER_KEY, LEADER_TTL);
            if (!isLeader.get()) {
                isLeader.set(true);
                log.info("[{}] *** Leadership RENEWED ***", instanceId);
            }
        } else if (currentLeader == null) {
            // Key expired or no leader — try to acquire
            Boolean acquired = redisTemplate.opsForValue()
                .setIfAbsent(LEADER_KEY, instanceId, LEADER_TTL);
            if (Boolean.TRUE.equals(acquired)) {
                isLeader.set(true);
                log.info("[{}] *** Leadership ACQUIRED — this instance is now the LEADER ***", instanceId);
            } else {
                isLeader.set(false);
            }
        } else {
            // Another instance holds the lock
            if (isLeader.getAndSet(false)) {
                log.info("[{}] Leadership lost to {}", instanceId, currentLeader);
            }
        }
    }

    public boolean isLeader() {
        return isLeader.get();
    }

    public String getCurrentLeader() {
        return redisTemplate.opsForValue().get(LEADER_KEY);
    }

    public String getInstanceId() {
        return instanceId;
    }
}
