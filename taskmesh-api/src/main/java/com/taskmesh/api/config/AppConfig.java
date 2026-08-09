package com.taskmesh.api.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class AppConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    // Pre-create Kafka topics on API startup
    @Bean public NewTopic jobsCreated()    { return TopicBuilder.name("taskmesh.jobs.created").partitions(3).replicas(1).build(); }
    @Bean public NewTopic jobsAssigned()   { return TopicBuilder.name("taskmesh.jobs.assigned").partitions(3).replicas(1).build(); }
    @Bean public NewTopic jobsCompleted()  { return TopicBuilder.name("taskmesh.jobs.completed").partitions(3).replicas(1).build(); }
    @Bean public NewTopic jobsFailed()     { return TopicBuilder.name("taskmesh.jobs.failed").partitions(3).replicas(1).build(); }
    @Bean public NewTopic jobsDeadLetter() { return TopicBuilder.name("taskmesh.jobs.dead-letter").partitions(1).replicas(1).build(); }
}
