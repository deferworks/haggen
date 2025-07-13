package co.deferworks.haggen.driver;

import co.deferworks.haggen.core.JobHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class WorkerPool {

    private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

    private final PostgresJobRepository jobRepository;
    private final JobHandler jobHandler;
    private final int numberOfWorkers;
    private final RetryStrategy retryStrategy;
    private ExecutorService executorService;

    public WorkerPool(PostgresJobRepository jobRepository, JobHandler jobHandler, int numberOfWorkers) {
        this(jobRepository, jobHandler, numberOfWorkers, new ExponentialBackoffRetryStrategy());
    }

    public WorkerPool(PostgresJobRepository jobRepository, JobHandler jobHandler, int numberOfWorkers, RetryStrategy retryStrategy) {
        this.jobRepository = jobRepository;
        this.jobHandler = jobHandler;
        this.numberOfWorkers = numberOfWorkers;
        this.retryStrategy = retryStrategy;
    }

    public void start() {
        log.info("Starting worker pool with {} workers.", numberOfWorkers);
        executorService = Executors.newVirtualThreadPerTaskExecutor();
        for (int i = 0; i < numberOfWorkers; i++) {
            executorService.submit(new Worker(UUID.randomUUID(), jobRepository, jobHandler, retryStrategy));
        }
    }

    public void shutdown() {
        log.info("Shutting down worker pool.");
        if (executorService != null) {
            executorService.shutdownNow();
            try {
                if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                    log.warn("Worker pool did not terminate in time.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Worker pool shutdown interrupted.", e);
            }
        }
    }
}