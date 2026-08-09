package com.taskmesh.worker.handler;

import com.taskmesh.common.enums.JobType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.concurrent.ThreadLocalRandom;

/** Simulates test suite execution (JUnit, pytest, etc.) */
@Component @Slf4j
public class TestExecutionHandler implements JobHandler {
    @Override
    public String execute(String payload, int timeoutSec) throws Exception {
        log.info("[TEST_EXECUTION] payload={}", payload);
        int ms = ThreadLocalRandom.current().nextInt(1000, 5000);
        Thread.sleep(Math.min(ms, timeoutSec * 900L));
        // 10% simulated failure rate for demo purposes
        if (ThreadLocalRandom.current().nextInt(10) == 0) {
            throw new RuntimeException("Test suite FAILED: 3 tests failed, 47 passed");
        }
        int passed = ThreadLocalRandom.current().nextInt(40, 100);
        return String.format("{\"status\":\"PASSED\",\"testsRun\":%d,\"failures\":0,\"durationMs\":%d}", passed, ms);
    }
    @Override public JobType getSupportedType() { return JobType.TEST_EXECUTION; }
}
