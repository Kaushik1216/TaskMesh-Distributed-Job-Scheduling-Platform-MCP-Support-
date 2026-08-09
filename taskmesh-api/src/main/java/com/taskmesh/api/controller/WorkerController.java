package com.taskmesh.api.controller;

import com.taskmesh.api.dto.WorkerRegistrationRequest;
import com.taskmesh.api.dto.WorkerResponse;
import com.taskmesh.api.service.WorkerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/workers")
@RequiredArgsConstructor
@Slf4j
public class WorkerController {

    private final WorkerService workerService;

    @PostMapping("/register")
    public ResponseEntity<WorkerResponse> register(@RequestBody WorkerRegistrationRequest request) {
        log.info("POST /api/v1/workers/register workerId={} caps={} maxConcurrency={}",
            request.getWorkerId(), request.getCapabilities(), request.getMaxConcurrency());
        return ResponseEntity.ok(workerService.registerOrUpdate(request));
    }

    @PostMapping("/{workerId}/heartbeat")
    public ResponseEntity<WorkerResponse> heartbeat(@PathVariable(name = "workerId") String workerId) {
        return workerService.heartbeat(workerId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public ResponseEntity<List<WorkerResponse>> getAllWorkers() {
        return ResponseEntity.ok(workerService.getAllWorkers());
    }

    @GetMapping("/{workerId}")
    public ResponseEntity<WorkerResponse> getWorker(@PathVariable(name = "workerId") String workerId) {
        return workerService.getWorker(workerId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }
}
