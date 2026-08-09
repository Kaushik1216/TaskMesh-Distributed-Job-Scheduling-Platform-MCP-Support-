package com.taskmesh.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EntityScan(basePackages = "com.taskmesh.common.entity")
@EnableJpaRepositories(basePackages = "com.taskmesh.worker.repository")
@EnableScheduling
public class TaskmeshWorkerApplication {
    public static void main(String[] args) {
        SpringApplication.run(TaskmeshWorkerApplication.class, args);
    }
}
