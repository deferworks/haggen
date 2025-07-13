package co.deferworks.haggen.driver;

import co.deferworks.haggen.core.JobHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

public class Worker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Worker.class);
    private static final int MAX_ATTEMPTS = 3;

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
                        if (job.attemptCount() < MAX_ATTEMPTS) {
                            OffsetDateTime runAt = OffsetDateTime.now().plus((long) Math.pow(2, job.attemptCount()), ChronoUnit.SECONDS);
                            jobRepository.markRetrying(job.id(), e.getMessage(), runAt);
                            log.info("Worker {} marked job {} for retry. Attempt: {}/{} ", workerId, job.id(), job.attemptCount() + 1, MAX_ATTEMPTS);
                        } else {
                            jobRepository.markDiscarded(job.id());
                            log.warn("Worker {} discarded job {} after {} attempts.", workerId, job.id(), job.attemptCount() + 1);
                        }
                    }
                });
                Thread.sleep(500); // Poll every 500ms.
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