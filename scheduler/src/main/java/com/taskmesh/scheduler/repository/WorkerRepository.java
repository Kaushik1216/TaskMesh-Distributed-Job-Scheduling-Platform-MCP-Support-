package com.taskmesh.scheduler.repository;

import com.taskmesh.common.entity.Worker;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface WorkerRepository extends JpaRepository<Worker, String> {
    List<Worker> findByStatus(String status);
}
