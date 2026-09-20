package com.taskmesh.worker.handler;

import com.taskmesh.common.enums.JobType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.concurrent.ThreadLocalRandom;

@Component @Slf4j
public class DataProcessingHandler implements JobHandler {
    @Override
    public String execute(String payload, int timeoutSec) throws Exception {
        log.info("[DATA_PROCESSING] payload={}", payload);
        Thread.sleep(ThreadLocalRandom.current().nextInt(1500, 6000));
        return "{\"status\":\"completed\",\"rowsProcessed\":50000,\"aggregations\":3}";
    }
    @Override public JobType getSupportedType() { return JobType.DATA_PROCESSING; }
}
