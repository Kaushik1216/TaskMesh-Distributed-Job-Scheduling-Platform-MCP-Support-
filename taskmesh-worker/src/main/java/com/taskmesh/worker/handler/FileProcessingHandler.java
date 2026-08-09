package com.taskmesh.worker.handler;

import com.taskmesh.common.enums.JobType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.concurrent.ThreadLocalRandom;

@Component @Slf4j
public class FileProcessingHandler implements JobHandler {
    @Override
    public String execute(String payload, int timeoutSec) throws Exception {
        log.info("[FILE_PROCESSING] payload={}", payload);
        Thread.sleep(ThreadLocalRandom.current().nextInt(1000, 4000));
        return "{\"status\":\"processed\",\"recordsProcessed\":10240,\"outputFile\":\"output.csv\"}";
    }
    @Override public JobType getSupportedType() { return JobType.FILE_PROCESSING; }
}
