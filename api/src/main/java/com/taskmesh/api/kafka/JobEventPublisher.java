package com.taskmesh.api.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskmesh.common.event.JobCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobEventPublisher {

    private static final String TOPIC_JOBS_CREATED = "taskmesh.jobs.created";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void publishJobCreated(JobCreatedEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(TOPIC_JOBS_CREATED, event.jobId().toString(), payload)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish JobCreatedEvent jobId={}", event.jobId(), ex);
                    } else {
                        log.info("Published JobCreatedEvent jobId={} partition={}",
                            event.jobId(), result.getRecordMetadata().partition());
                    }
                });
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize/publish job event", e);
        }
    }
}
