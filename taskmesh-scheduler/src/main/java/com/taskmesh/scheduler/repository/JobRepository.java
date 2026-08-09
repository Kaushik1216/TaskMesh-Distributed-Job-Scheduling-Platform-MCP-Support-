package com.taskmesh.scheduler.repository;

import com.taskmesh.common.entity.Job;
import com.taskmesh.common.enums.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface JobRepository extends JpaRepository<Job, UUID> {
    List<Job> findByStatusOrderByCreatedAtAsc(JobStatus status);
}
