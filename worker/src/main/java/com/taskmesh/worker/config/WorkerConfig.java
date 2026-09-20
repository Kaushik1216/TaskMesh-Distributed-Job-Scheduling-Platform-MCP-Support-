package com.taskmesh.worker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class WorkerConfig {

    @Value("${taskmesh.worker.id:worker-1}")
    private String workerId;

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Unique consumer group per worker instance.
     * This ensures ALL workers receive every JobAssignedEvent from Kafka,
     * letting each worker filter for its own assignments.
     */
    @Bean
    public String workerConsumerGroupId() {
        return "taskmesh-worker-" + workerId;
    }
}
