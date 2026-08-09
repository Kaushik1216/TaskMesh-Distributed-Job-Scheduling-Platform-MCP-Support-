package com.taskmesh.worker.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages worker liveness:
 * 1. Registers with the API on startup
 * 2. Sends heartbeat to Redis (TTL=30s) every 10 seconds
 * 3. Pings the API heartbeat endpoint to update PostgreSQL lastHeartbeat
 *
 * If the worker crashes, the Redis key expires after 30s and the
 * WorkerService.markDeadWorkers() scheduled job marks it as DEAD.
 * The scheduler will stop routing jobs to dead workers automatically.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class HeartbeatService {

    private static final Duration HEARTBEAT_TTL = Duration.ofSeconds(30);

    @Value("${taskmesh.worker.id:worker-1}")
    private String workerId;

    @Value("${taskmesh.worker.capabilities:RUN_COMMAND}")
    private String capabilities;

    @Value("${taskmesh.worker.max-concurrency:4}")
    private int maxConcurrency;

    @Value("${taskmesh.api.base-url:http://localhost:8080}")
    private String apiBaseUrl;

    private final StringRedisTemplate redisTemplate;
    private final RestTemplate        restTemplate;

    @EventListener(ApplicationReadyEvent.class)
    public void registerOnStartup() {
        log.info("[{}] Registering with API: caps={} maxConcurrency={}", workerId, capabilities, maxConcurrency);
        try {
            Map<String, Object> body = new HashMap<>();
            body.put("workerId", workerId);
            body.put("capabilities", capabilities);
            body.put("maxConcurrency", maxConcurrency);
            restTemplate.postForObject(apiBaseUrl + "/api/v1/workers/register", body, Map.class);
            log.info("[{}] Registration successful", workerId);
        } catch (Exception e) {
            log.warn("[{}] Registration failed (API may not be ready yet): {}", workerId, e.getMessage());
        }
    }

    /**
     * Dual heartbeat every 10 seconds:
     * - Redis key for fast TTL-based dead detection
     * - API endpoint for DB persistence
     */
    @Scheduled(fixedDelay = 10_000)
    public void sendHeartbeat() {
        // Redis heartbeat — fast path
        String redisKey = "taskmesh:worker:" + workerId;
        redisTemplate.opsForValue().set(redisKey, "alive", HEARTBEAT_TTL);
        log.debug("[{}] Redis heartbeat refreshed (TTL={}s)", workerId, HEARTBEAT_TTL.getSeconds());

        // API heartbeat — updates lastHeartbeat in PostgreSQL
        try {
            restTemplate.postForObject(
                apiBaseUrl + "/api/v1/workers/" + workerId + "/heartbeat", null, Map.class);
        } catch (Exception e) {
            log.warn("[{}] API heartbeat failed: {}", workerId, e.getMessage());
        }
    }
}
