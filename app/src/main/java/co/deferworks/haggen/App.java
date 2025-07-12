package co.deferworks.haggen;

import co.deferworks.haggen.core.Job;
import co.deferworks.haggen.core.JobHandler;
import co.deferworks.haggen.driver.PostgresDriver;
import co.deferworks.haggen.driver.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        String jdbcUrl = "jdbc:postgresql://localhost:5432/haggen-test";
        String username = "test";
        String password = "test";

        // Simple JobHandler implementation
        JobHandler myJobHandler = job -> {
            log.info("Handling job: {} with kind {}", job.id(), job.kind());
            // Simulate some work
            TimeUnit.SECONDS.sleep(2);
            log.info("Finished handling job: {}", job.id());
        };

        PostgresDriver driver = new PostgresDriver(jdbcUrl, username, password, myJobHandler, 5);

        try {
            driver.start();
            Queue queue = driver.getQueue();

            // Enqueue a sample job
            Job sampleJob = Job.builder()
                    .kind("my-sample-job")
                    .queue("default")
                    .metadata("{\"data\": \"some value\"}")
                    .build();
            Job enqueuedJob = queue.enqueue(sampleJob);
            log.info("Enqueued job with ID: {}", enqueuedJob.id());

            // Keep the application running for a bit to allow workers to process jobs
            TimeUnit.SECONDS.sleep(10);

        } catch (Exception e) {
            log.error("Application error: ", e);
        } finally {
            driver.shutdown();
        }
    }
}
