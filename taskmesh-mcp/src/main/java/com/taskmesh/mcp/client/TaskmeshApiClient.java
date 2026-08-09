package com.taskmesh.mcp.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * HTTP client for calling the TaskMesh REST API.
 * The MCP server acts as an AI-friendly facade over the REST API.
 * All tool calls go through this client.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class TaskmeshApiClient {

    @Value("${taskmesh.api.base-url:http://localhost:8080}")
    private String baseUrl;

    private final RestTemplate restTemplate;

    public String createJob(String type, String priority, String payload,
                            String requiredCapabilities, int maxRetries, int timeoutSeconds) {
        try {
            Map<String, Object> body = new HashMap<>();
            body.put("type", type);
            body.put("priority", priority != null && !priority.isBlank() ? priority : "NORMAL");
            body.put("payload", payload != null ? payload : "{}");
            body.put("requiredCapabilities", requiredCapabilities != null ? requiredCapabilities : "");
            body.put("maxRetries", maxRetries > 0 ? maxRetries : 3);
            body.put("timeoutSeconds", timeoutSeconds > 0 ? timeoutSeconds : 300);

            ResponseEntity<String> response = restTemplate.postForEntity(
                baseUrl + "/api/v1/jobs", body, String.class);
            log.info("[MCP] create_job → {}", response.getStatusCode());
            return response.getBody();
        } catch (Exception e) {
            log.error("[MCP] create_job failed", e);
            return "{\"error\": \"" + e.getMessage() + "\"}";
        }
    }

    public String getJob(String jobId) {
        try {
            return restTemplate.getForObject(baseUrl + "/api/v1/jobs/" + jobId, String.class);
        } catch (HttpClientErrorException.NotFound e) {
            return "{\"error\": \"Job not found: " + jobId + "\"}";
        } catch (Exception e) {
            return "{\"error\": \"" + e.getMessage() + "\"}";
        }
    }

    public String listJobs(String status) {
        try {
            String url = baseUrl + "/api/v1/jobs";
            if (status != null && !status.isBlank()) url += "?status=" + status.toUpperCase();
            return restTemplate.getForObject(url, String.class);
        } catch (Exception e) {
            return "{\"error\": \"" + e.getMessage() + "\"}";
        }
    }

    public String cancelJob(String jobId) {
        try {
            restTemplate.delete(baseUrl + "/api/v1/jobs/" + jobId);
            return "{\"message\": \"Job " + jobId + " cancelled successfully\"}";
        } catch (Exception e) {
            return "{\"error\": \"" + e.getMessage() + "\"}";
        }
    }

    public String retryJob(String jobId) {
        try {
            ResponseEntity<String> response = restTemplate.postForEntity(
                baseUrl + "/api/v1/jobs/" + jobId + "/retry", null, String.class);
            return response.getBody();
        } catch (Exception e) {
            return "{\"error\": \"" + e.getMessage() + "\"}";
        }
    }

    public String listWorkers() {
        try {
            return restTemplate.getForObject(baseUrl + "/api/v1/workers", String.class);
        } catch (Exception e) {
            return "{\"error\": \"" + e.getMessage() + "\"}";
        }
    }
}
