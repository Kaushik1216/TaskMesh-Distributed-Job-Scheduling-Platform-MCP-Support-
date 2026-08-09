package com.taskmesh.worker.repository;

import com.taskmesh.common.entity.Job;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.UUID;

@Repository
public interface JobRepository extends JpaRepository<Job, UUID> {}
