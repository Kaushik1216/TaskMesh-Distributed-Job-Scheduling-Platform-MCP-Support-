package com.taskmesh.api.dto;

import lombok.Data;

@Data
public class WorkerRegistrationRequest {
    private String workerId;
    private String capabilities;
    private int maxConcurrency = 4;
}
