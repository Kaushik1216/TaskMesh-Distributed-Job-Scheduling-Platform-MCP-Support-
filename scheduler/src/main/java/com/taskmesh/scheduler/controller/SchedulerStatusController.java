package com.taskmesh.scheduler.controller;

import com.taskmesh.scheduler.service.LeaderElectionService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/scheduler")
@RequiredArgsConstructor
public class SchedulerStatusController {

    private final LeaderElectionService leaderElection;

    @GetMapping("/status")
    public Map<String, Object> status() {
        return Map.of(
            "instanceId", leaderElection.getInstanceId(),
            "isLeader", leaderElection.isLeader(),
            "currentLeader", leaderElection.getCurrentLeader() != null
                ? leaderElection.getCurrentLeader() : "none"
        );
    }
}
