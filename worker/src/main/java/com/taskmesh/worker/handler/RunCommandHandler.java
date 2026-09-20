package com.taskmesh.worker.handler;

import com.taskmesh.common.enums.JobType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.concurrent.ThreadLocalRandom;

@Component @Slf4j
public class RunCommandHandler implements JobHandler {
    @Override
    public String execute(String payload, int timeoutSec) throws Exception {
        log.info("[RUN_COMMAND] payload={}", payload);
        Thread.sleep(ThreadLocalRandom.current().nextInt(500, 3000));
        return "{\"exitCode\":0,\"stdout\":\"Command executed successfully\",\"stderr\":\"\"}";
    }
    @Override public JobType getSupportedType() { return JobType.RUN_COMMAND; }
}
