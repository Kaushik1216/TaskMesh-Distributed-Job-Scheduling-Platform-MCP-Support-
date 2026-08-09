package com.taskmesh.worker.handler;

import com.taskmesh.common.enums.JobType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.concurrent.ThreadLocalRandom;

@Component @Slf4j
public class BuildProjectHandler implements JobHandler {
    @Override
    public String execute(String payload, int timeoutSec) throws Exception {
        log.info("[BUILD_PROJECT] payload={}", payload);
        Thread.sleep(ThreadLocalRandom.current().nextInt(2000, 8000));
        return "{\"status\":\"BUILD_SUCCESS\",\"artifact\":\"app-1.0.0.jar\",\"sizeMB\":12.4}";
    }
    @Override public JobType getSupportedType() { return JobType.BUILD_PROJECT; }
}
