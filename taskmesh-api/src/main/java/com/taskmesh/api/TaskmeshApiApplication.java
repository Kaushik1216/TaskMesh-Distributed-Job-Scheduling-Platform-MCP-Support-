package com.taskmesh.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EntityScan(basePackages = "com.taskmesh.common.entity")
@EnableScheduling
public class TaskmeshApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(TaskmeshApiApplication.class, args);
    }
}
