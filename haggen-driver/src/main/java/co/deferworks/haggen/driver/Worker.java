package co.deferworks.haggen.driver;

import co.deferworks.haggen.core.JobHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class Worker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    private final UUID workerId;
    private final PostgresJobRepository jobRepository;
    private final JobHandler jobHandler;

    public Worker(UUID workerId, PostgresJobRepository jobRepository, JobHandler jobHandler) {
        this.workerId = workerId;
        this.jobRepository = jobRepository;
        this.jobHandler = jobHandler;
    }

    @Override
    public void run() {
        log.info("Worker {} started.", workerId);
        while (!Thread.currentThread().isInterrupted()) {
            try {
                jobRepository.fetchAndLockJob(workerId).ifPresent(job -> {
                    log.info("Worker {} fetched job: {}", workerId, job.id());
                    try {
                        jobHandler.handle(job);
                        jobRepository.markComplete(job.id());
                        log.info("Worker {} completed job: {}", workerId, job.id());
                    } catch (Exception e) {
                        log.error("Worker {} failed to process job {}: {}", workerId, job.id(), e.getMessage(), e);
                        jobRepository.markFailed(job.id(), e.getMessage());
                    }
                });
                Thread.sleep(500); // Poll every second
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("Worker {} interrupted.", workerId);
            } catch (Exception e) {
                log.error("Worker {} encountered an unexpected error: {}", workerId, e.getMessage(), e);
            }
        }
        log.info("Worker {} stopped.", workerId);
    }
}