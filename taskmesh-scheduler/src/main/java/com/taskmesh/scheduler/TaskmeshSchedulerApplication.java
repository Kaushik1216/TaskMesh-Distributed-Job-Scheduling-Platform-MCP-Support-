package com.taskmesh.scheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EntityScan(basePackages = "com.taskmesh.common.entity")
@EnableJpaRepositories(basePackages = "com.taskmesh.scheduler.repository")
@EnableScheduling
public class TaskmeshSchedulerApplication {
    public static void main(String[] args) {
        SpringApplication.run(TaskmeshSchedulerApplication.class, args);
    }
}
