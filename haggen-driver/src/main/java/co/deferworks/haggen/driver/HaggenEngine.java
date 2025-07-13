package co.deferworks.haggen.driver;

import co.deferworks.haggen.core.JobHandler;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HaggenEngine {

    private static final Logger log = LoggerFactory.getLogger(HaggenEngine.class);

    private final PostgresJobRepository jobRepository;
    private final PostgresQueue queue;
    private final WorkerPool workerPool;
    private final ScheduledExecutorService reaperScheduler;
    private final HookRegistry hookRegistry;

    public HaggenEngine(HikariDataSource dataSource, JobHandler jobHandler, int numberOfWorkers, HookRegistry hookRegistry) {
        this(dataSource, jobHandler, numberOfWorkers, hookRegistry, new ExponentialBackoffRetryStrategy());
    }

    public HaggenEngine(HikariDataSource dataSource, JobHandler jobHandler, int numberOfWorkers, HookRegistry hookRegistry, RetryStrategy retryStrategy) {
        this.hookRegistry = hookRegistry;
        this.jobRepository = new PostgresJobRepository(dataSource, hookRegistry);
        this.queue = new PostgresQueue(jobRepository);
        this.workerPool = new WorkerPool(jobRepository, jobHandler, numberOfWorkers, retryStrategy);
        this.reaperScheduler = Executors.newSingleThreadScheduledExecutor();
    }

    public Queue getQueue() {
        return queue;
    }

    public void start() {
        log.info("Starting Haggen Engine...");
        workerPool.start();
        // Schedule the reaper to run every 5 minutes
        reaperScheduler.scheduleAtFixedRate(jobRepository::reapStaleJobs, 0, 5, TimeUnit.MINUTES);
        log.info("Haggen Engine started.");
    }

    public void shutdown() {
        log.info("Shutting down Haggen Engine...");
        workerPool.shutdown();
        reaperScheduler.shutdown();
        try {
            if (!reaperScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Reaper scheduler did not terminate in time.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Reaper scheduler shutdown interrupted.", e);
        }
        log.info("Haggen Engine shut down.");
    }
}