package com.taskmesh.worker.handler;

import com.taskmesh.common.enums.JobType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.concurrent.ThreadLocalRandom;

@Component @Slf4j
public class HttpRequestHandler implements JobHandler {
    @Override
    public String execute(String payload, int timeoutSec) throws Exception {
        log.info("[HTTP_REQUEST] payload={}", payload);
        Thread.sleep(ThreadLocalRandom.current().nextInt(200, 2000));
        return "{\"statusCode\":200,\"responseTimeMs\":142,\"contentLength\":1024}";
    }
    @Override public JobType getSupportedType() { return JobType.HTTP_REQUEST; }
}
