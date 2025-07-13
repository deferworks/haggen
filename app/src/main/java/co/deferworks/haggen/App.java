package co.deferworks.haggen;

import co.deferworks.haggen.core.Job;
import co.deferworks.haggen.core.JobHandler;
import co.deferworks.haggen.db.DatabaseMigrations;
import co.deferworks.haggen.driver.HookRegistry;
import co.deferworks.haggen.driver.PostgresDriver;
import co.deferworks.haggen.driver.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.concurrent.TimeUnit;

public class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        String jdbcUrl = System.getenv("JDBC_URL");
        String username = System.getenv("DB_USER");
        String password = System.getenv("DB_PASSWORD");

        // Run Flyway migrations
        DatabaseMigrations.runMigrations(jdbcUrl, username, password);

        PostgresDriver driver = getPostgresDriver(jdbcUrl, username, password);

        try {
            driver.start();
            Queue queue = driver.getQueue();

            // Enqueue a sample job (non-transactional)
            Job sampleJob = Job.builder()
                    .kind("my-sample-job")
                    .queue("default")
                    .metadata("{\"data\": \"some value\"}")
                    .build();
            Job enqueuedJob = queue.enqueue(sampleJob);
            log.info("Enqueued job with ID: {} (non-transactional)", enqueuedJob.id());

            // Enqueue a failing job to test retries and discard
            Job failingJob = Job.builder()
                    .kind("failing-job")
                    .queue("default")
                    .metadata("{\"data\": \"this job will fail\"}")
                    .build();
            Job enqueuedFailingJob = queue.enqueue(failingJob);
            log.info("Enqueued failing job with ID: {}", enqueuedFailingJob.id());

            // Enqueue a sample job (transactional)
            try (Connection connection = driver.getConnection()) {
                connection.setAutoCommit(false);

                Job transactionalJob = Job.builder()
                        .kind("my-transactional-job")
                        .queue("default")
                        .metadata("{\"data\": \"transactional value\"}")
                        .build();
                log.info("Built transactional job: {}", transactionalJob);
                Job enqueuedTransactionalJob = queue.enqueue(transactionalJob, connection);
                log.info("Enqueued job with ID: {} (transactional)", enqueuedTransactionalJob.id());

                // Simulate some other transactional work here
                // If an error occurs before commit, the job enqueue will be rolled back

                connection.commit();
                log.info("Transaction committed for job: {}", enqueuedTransactionalJob.id());
            } catch (Exception e) {
                log.error("Transactional enqueue failed: ", e);
            }

            // Keep the application running for a bit to allow workers to process jobs
            TimeUnit.SECONDS.sleep(10);

        } catch (Exception e) {
            log.error("Application error: ", e);
        } finally {
            driver.shutdown();
        }
    }

    private static PostgresDriver getPostgresDriver(String jdbcUrl, String username, String password) {
        // Create a HookRegistry.
        HookRegistry hookRegistry = getHookRegistry();

        // Simple JobHandler implementation
        JobHandler myJobHandler = job -> {
            log.info("Handling job: {} with kind {}", job.id(), job.kind());
            if (job.kind().equals("failing-job")) {
                throw new RuntimeException("Simulated job failure");
            }
            // Simulate some work
            TimeUnit.SECONDS.sleep(2);
            log.info("Finished handling job: {}", job.id());
        };

        PostgresDriver driver = new PostgresDriver(jdbcUrl, username, password, myJobHandler, 5, hookRegistry);
        return driver;
    }

    private static HookRegistry getHookRegistry() {
        HookRegistry hookRegistry = new HookRegistry();

        // Register some sample hooks
        hookRegistry.registerOnEnqueue("my-sample-job", job -> log.info("Hook: Job {} enqueued.", job.id()));
        hookRegistry.registerOnDequeue("my-sample-job", job -> log.info("Hook: Job {} dequeued.", job.id()));
        hookRegistry.registerOnComplete("my-sample-job", job -> log.info("Hook: Job {} completed.", job.id()));
        hookRegistry.registerOnFail("my-sample-job", job -> log.warn("Hook: Job {} failed.", job.id()));
        hookRegistry.registerOnReap("my-sample-job", job -> log.info("Hook: Job {} reaped.", job.id()));

        hookRegistry.registerOnEnqueue("my-transactional-job", job -> log.info("Hook: Transactional Job {} enqueued.", job.id()));
        hookRegistry.registerOnDequeue("my-transactional-job", job -> log.info("Hook: Transactional Job {} dequeued.", job.id()));
        hookRegistry.registerOnComplete("my-transactional-job", job -> log.info("Hook: Transactional Job {} completed.", job.id()));
        hookRegistry.registerOnFail("my-transactional-job", job -> log.warn("Hook: Transactional Job {} failed.", job.id()));
        hookRegistry.registerOnReap("my-transactional-job", job -> log.info("Hook: Transactional Job {} reaped.", job.id()));

        hookRegistry.registerOnEnqueue("failing-job", job -> log.info("Hook: Failing Job {} enqueued.", job.id()));
        hookRegistry.registerOnDequeue("failing-job", job -> log.info("Hook: Failing Job {} dequeued.", job.id()));
        hookRegistry.registerOnFail("failing-job", job -> log.warn("Hook: Failing Job {} failed. Attempt: {}", job.id(), job.attemptCount()));
        hookRegistry.registerOnReap("failing-job", job -> log.info("Hook: Failing Job {} reaped.", job.id()));
        hookRegistry.registerOnDiscard("failing-job", job -> log.error("Hook: Failing Job {} DISCARDED after max retries.", job.id()));

        return hookRegistry;
    }
}

