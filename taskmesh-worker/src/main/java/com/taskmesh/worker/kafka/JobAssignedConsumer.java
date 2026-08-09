package com.taskmesh.worker.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskmesh.common.entity.Job;
import com.taskmesh.common.enums.JobStatus;
import com.taskmesh.common.event.JobAssignedEvent;
import com.taskmesh.worker.executor.ConcurrentJobExecutor;
import com.taskmesh.worker.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Kafka consumer for job assignments.
 *
 * Each worker instance gets its own consumer group (group-id = "taskmesh-worker-{workerId}").
 * This means ALL workers receive every JobAssignedEvent, but each worker
 * only processes the message if it was assigned to THIS worker's ID.
 *
 * This pattern allows the scheduler to target specific workers while
 * still using Kafka's reliable message delivery guarantees.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class JobAssignedConsumer {

    @Value("${taskmesh.worker.id:worker-1}")
    private String workerId;

    private final ConcurrentJobExecutor jobExecutor;
    private final JobRepository         jobRepository;
    private final ObjectMapper          objectMapper;

    @KafkaListener(
        topics = "taskmesh.jobs.assigned",
        groupId = "#{@workerConsumerGroupId}"  // unique group per worker
    )
    public void onJobAssigned(ConsumerRecord<String, String> record) {
        try {
            JobAssignedEvent event = objectMapper.readValue(record.value(), JobAssignedEvent.class);

            // Ignore assignments meant for other workers
            if (!workerId.equals(event.workerId())) return;

            log.info("[{}] Received assignment for jobId={}", workerId, event.jobId());

            Optional<Job> jobOpt = jobRepository.findById(event.jobId());
            if (jobOpt.isEmpty()) {
                log.warn("[{}] Job not found in DB: {}", workerId, event.jobId());
                return;
            }

            Job job = jobOpt.get();
            if (job.getStatus() == JobStatus.CANCELLED) {
                log.info("[{}] Job {} already CANCELLED — skipping", workerId, event.jobId());
                return;
            }

            jobExecutor.submitJob(job);

        } catch (Exception e) {
            log.error("[{}] Error processing JobAssignedEvent key={}", workerId, record.key(), e);
        }
    }
}
