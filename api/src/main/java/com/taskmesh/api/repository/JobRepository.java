package com.taskmesh.api.repository;

import com.taskmesh.common.entity.Job;
import com.taskmesh.common.enums.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface JobRepository extends JpaRepository<Job, UUID> {

    Optional<Job> findByIdempotencyKey(String idempotencyKey);

    List<Job> findByStatusOrderByCreatedAtDesc(JobStatus status);

    List<Job> findAllByOrderByCreatedAtDesc();

    List<Job> findByAssignedWorkerIdAndStatusIn(String workerId, List<JobStatus> statuses);
}
