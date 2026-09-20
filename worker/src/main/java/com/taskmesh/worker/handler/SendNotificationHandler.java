package com.taskmesh.worker.handler;

import com.taskmesh.common.enums.JobType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.concurrent.ThreadLocalRandom;

@Component @Slf4j
public class SendNotificationHandler implements JobHandler {
    @Override
    public String execute(String payload, int timeoutSec) throws Exception {
        log.info("[SEND_NOTIFICATION] payload={}", payload);
        Thread.sleep(ThreadLocalRandom.current().nextInt(100, 500));
        return "{\"status\":\"sent\",\"recipients\":1,\"channel\":\"email\"}";
    }
    @Override public JobType getSupportedType() { return JobType.SEND_NOTIFICATION; }
}
