package com.taskmesh.api.repository;

import com.taskmesh.common.entity.Worker;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface WorkerRepository extends JpaRepository<Worker, String> {

    List<Worker> findByStatus(String status);

    @Query("SELECT w FROM Worker w WHERE w.lastHeartbeat < :threshold AND w.status = 'ACTIVE'")
    List<Worker> findDeadWorkers(@Param("threshold") Instant threshold);
}
