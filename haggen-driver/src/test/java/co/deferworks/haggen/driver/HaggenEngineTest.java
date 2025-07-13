package co.deferworks.haggen.driver;

import co.deferworks.haggen.core.Job;
import co.deferworks.haggen.core.JobHandler;
import co.deferworks.haggen.db.DatabaseMigrations;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class HaggenEngineTest {

    @Container
    private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16.9")
            .withDatabaseName("haggen-test")
            .withUsername("test")
            .withPassword("test");

    private HikariDataSource dataSource;
    private PostgresJobRepository jobRepository;
    private HaggenEngine engine;

    @BeforeAll
    static void beforeAll() {
        postgres.start();
        Flyway flyway = Flyway.configure(DatabaseMigrations.class.getClassLoader())
                .dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                .locations("classpath:db/migration").load();
        flyway.migrate();
    }

    @AfterAll
    static void afterAll() {
        postgres.stop();
    }

    @BeforeEach
    void setUp() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        dataSource = new HikariDataSource(config);
        jobRepository = new PostgresJobRepository(dataSource);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (engine != null) {
            engine.shutdown();
        }
        if (dataSource != null) {
            try (var connection = dataSource.getConnection();
                 var statement = connection.createStatement()) {
                statement.executeUpdate("TRUNCATE TABLE jobs");
            } catch (Exception e) {
                // Ignore
            }
            dataSource.close();
        }
    }

    @Test
    void testEndToEndWorkerRetryAndDiscardFlow() throws InterruptedException {
        final int MAX_ATTEMPTS = 3;
        final String JOB_KIND = "failing-job";
        final CountDownLatch retryLatch = new CountDownLatch(MAX_ATTEMPTS); // Fails
        final CountDownLatch discardLatch = new CountDownLatch(1); // Discard

        final Map<UUID, AtomicInteger> failureCounts = new ConcurrentHashMap<>();

        JobHandler failingJobHandler = job -> {
            failureCounts.computeIfAbsent(job.id(), k -> new AtomicInteger(0)).incrementAndGet();
            throw new RuntimeException("Simulated failure");
        };

        HookRegistry hookRegistry = new HookRegistry();
        hookRegistry.registerOnFail(JOB_KIND, job -> retryLatch.countDown());
        hookRegistry.registerOnDiscard(JOB_KIND, job -> discardLatch.countDown());

        jobRepository = new PostgresJobRepository(dataSource, hookRegistry);
        engine = new HaggenEngine(dataSource, failingJobHandler, 1, hookRegistry, job -> OffsetDateTime.now());

        engine.start();

        Queue queue = new PostgresQueue(jobRepository);
        Job job = Job.builder().kind(JOB_KIND).queue("default").build();
        Job enqueuedJob = queue.enqueue(job);

        assertTrue(discardLatch.await(10, TimeUnit.SECONDS));

        Job finalJobState = jobRepository.findById(enqueuedJob.id()).orElseThrow();
        assertEquals(Job.JobState.DISCARDED, finalJobState.state());
        assertEquals(MAX_ATTEMPTS, finalJobState.attemptCount());
        assertEquals(MAX_ATTEMPTS + 1, failureCounts.get(enqueuedJob.id()).get());
    }

    @Test
    void testEndToEndWorkerScaleCorrectly() throws InterruptedException {
        final ConcurrentHashMap<UUID, Integer> completedJobs = new ConcurrentHashMap<UUID, Integer>();
        final CountDownLatch completedLatch = new CountDownLatch(100);
        final String JOB_KIND = "scaled-out-jobs";

        JobHandler countingJobHandler = job -> {
            completedJobs.computeIfAbsent(job.lockedBy(), k -> 0);
            completedJobs.computeIfPresent(job.lockedBy(), (k, v) -> v + 1);
        };

        HookRegistry hookRegistry = new HookRegistry();
        hookRegistry.registerOnComplete(JOB_KIND, job -> completedLatch.countDown());

        jobRepository = new PostgresJobRepository(dataSource, hookRegistry);
        engine = new HaggenEngine(dataSource, countingJobHandler, 20, hookRegistry);

        engine.start();

        Queue queue = new PostgresQueue(jobRepository);

        IntStream.range(0, 100).forEach(i -> {
            Job job = Job.builder().kind(JOB_KIND).queue("default").build();
            Job enqueued = queue.enqueue(job);
        });

        assertTrue(completedLatch.await(30, TimeUnit.SECONDS));

        var actualCompletedJobsCount = completedJobs.reduceValuesToInt(1, k -> k, 0, Integer::sum);

        assertEquals(100, actualCompletedJobsCount);
    }
}
