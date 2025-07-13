package co.deferworks.haggen.driver;

import co.deferworks.haggen.core.Job;
import co.deferworks.haggen.core.Job.JobLeaseKind;
import co.deferworks.haggen.core.Job.JobPriority;
import co.deferworks.haggen.core.Job.JobState;
import co.deferworks.haggen.db.DatabaseMigrations;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgresJobRepositoryTest {

    @Container
    private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16.9")
            .withDatabaseName("haggen-test")
            .withUsername("test")
            .withPassword("test");

    private HikariDataSource dataSource;
    private PostgresJobRepository jobRepository;

    private static final Logger logger = LoggerFactory.getLogger(PostgresJobRepositoryTest.class);

    @BeforeAll
    static void ensureContainerIsRunning() {
        postgres.start();
        assertTrue(postgres.isRunning());
        // Apply Flyway migrations
        Flyway flyway = Flyway.configure(DatabaseMigrations.class.getClassLoader())
                .dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                .locations("classpath:db/migration").load();
        flyway.migrate();
    }

    @AfterAll
    static void shutdownContainer() {
        postgres.stop();
        assertFalse(postgres.isRunning());
    }

    @BeforeEach
    void setUp() throws SQLException {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());

        dataSource = new HikariDataSource(config);
        jobRepository = new PostgresJobRepository(dataSource);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (dataSource != null) {
            // Clean up existing jobs.
            var conn = dataSource.getConnection();
            conn.createStatement().executeUpdate("TRUNCATE TABLE jobs;");
            dataSource.close();
        }
    }

    @Test
    void testCreateAndFindJob() {
        Job newJob = Job.builder()
                .kind("test-kind")
                .queue("default")
                .metadata("{}")
                .priority(JobPriority.NORMAL)
                .state(JobState.QUEUED)
                .runAt(OffsetDateTime.now())
                .createdAt(OffsetDateTime.now())
                .attemptCount(0)
                .lastErrorMessage(null)
                .lastErrorDetails(null)
                .leaseKind(JobLeaseKind.EXPIRABLE)
                .lockedBy(null)
                .lockedAt(null)
                .leaseToken(null)
                .build();

        Job createdJob = jobRepository.create(newJob);

        assertNotNull(createdJob.id());
        assertEquals("test-kind", createdJob.kind());
        assertEquals("default", createdJob.queue());
        assertEquals(JobPriority.NORMAL, createdJob.priority());
        assertEquals(JobState.QUEUED, createdJob.state());
        assertEquals("{}", createdJob.metadata());

        Optional<Job> foundJob = jobRepository.findById(createdJob.id());
        assertTrue(foundJob.isPresent());
        assertEquals(createdJob, foundJob.get());
    }

    @Test
    void testFetchAndLockJob() {
        // Create a job to be fetched
        Job jobToFetch = Job.builder()
                .kind("fetch-kind")
                .queue("default")
                .metadata("{}")
                .priority(JobPriority.HIGH)
                .state(JobState.QUEUED)
                .runAt(OffsetDateTime.now())
                .createdAt(OffsetDateTime.now())
                .attemptCount(0)
                .lastErrorMessage(null)
                .lastErrorDetails(null)
                .leaseKind(JobLeaseKind.EXPIRABLE)
                .lockedBy(null)
                .lockedAt(null)
                .leaseToken(null)
                .build();
        jobRepository.create(jobToFetch);

        UUID workerId = UUID.randomUUID();
        Optional<Job> fetchedJob = jobRepository.fetchAndLockJob(workerId);

        assertTrue(fetchedJob.isPresent());
        assertEquals(JobState.RUNNING, fetchedJob.get().state());
        assertEquals(workerId, fetchedJob.get().lockedBy());
        assertNotNull(fetchedJob.get().lockedAt());

        // Verify that the job is no longer QUEUED
        Optional<Job> originalJob = jobRepository.findById(fetchedJob.get().id());
        assertTrue(originalJob.isPresent());
        assertEquals(JobState.RUNNING, originalJob.get().state());
    }

    @Test
    void testMarkComplete() {
        Job jobToComplete = Job.builder()
                .kind("complete-kind")
                .queue("default")
                .metadata("{}")
                .priority(JobPriority.LOW)
                .state(JobState.QUEUED)
                .runAt(OffsetDateTime.now())
                .createdAt(OffsetDateTime.now())
                .attemptCount(0)
                .lastErrorMessage(null)
                .lastErrorDetails(null)
                .leaseKind(JobLeaseKind.EXPIRABLE)
                .lockedBy(null)
                .lockedAt(null)
                .leaseToken(null)
                .build();
        Job createdJob = jobRepository.create(jobToComplete);

        jobRepository.markComplete(createdJob.id());

        Optional<Job> completedJob = jobRepository.findById(createdJob.id());
        assertTrue(completedJob.isPresent());
        assertEquals(JobState.COMPLETED, completedJob.get().state());
    }

    @Test
    void testMarkFailed() {
        Job jobToFail = Job.builder()
                .kind("fail-kind")
                .queue("default")
                .metadata("{}")
                .priority(JobPriority.NORMAL)
                .state(JobState.QUEUED)
                .runAt(OffsetDateTime.now())
                .createdAt(OffsetDateTime.now())
                .attemptCount(0)
                .lastErrorMessage(null)
                .lastErrorDetails(null)
                .leaseKind(JobLeaseKind.EXPIRABLE)
                .lockedBy(null)
                .lockedAt(null)
                .leaseToken(null)
                .build();
        Job createdJob = jobRepository.create(jobToFail);

        String errorMessage = "Simulated failure";
        jobRepository.markFailed(createdJob.id(), errorMessage);

        Optional<Job> failedJob = jobRepository.findById(createdJob.id());
        assertTrue(failedJob.isPresent());
        assertEquals(JobState.FAILED, failedJob.get().state());
        assertEquals(errorMessage, failedJob.get().lastErrorMessage());
    }

    @Test
    void testReapStaleJobs() throws InterruptedException {
        // Create a job that should be reaped (RUNNING, EXPIRABLE, old locked_at)
        Job staleJob = Job.builder()
                .kind("stale-kind")
                .queue("default")
                .metadata("{}")
                .priority(JobPriority.NORMAL)
                .state(JobState.RUNNING)
                .runAt(OffsetDateTime.now())
                .createdAt(OffsetDateTime.now())
                .attemptCount(0)
                .lastErrorMessage(null)
                .lastErrorDetails(null)
                .leaseKind(JobLeaseKind.EXPIRABLE)
                .lockedBy(null)
                .lockedAt(OffsetDateTime.now().minusHours(2))
                .leaseToken(null)
                .build();
        Job createdStaleJob = jobRepository.create(staleJob);

        // Create a job that should NOT be reaped (RUNNING, PERMANENT lease)
        Job permanentJob = Job.builder()
                .kind("permanent-kind")
                .queue("default")
                .metadata("{}")
                .priority(JobPriority.NORMAL)
                .state(JobState.RUNNING)
                .runAt(OffsetDateTime.now())
                .createdAt(OffsetDateTime.now())
                .attemptCount(0)
                .lastErrorMessage(null)
                .lastErrorDetails(null)
                .leaseKind(JobLeaseKind.PERMANENT)
                .lockedBy(null)
                .lockedAt(OffsetDateTime.now().minusHours(2))
                .leaseToken(null)
                .build();
        Job createdPermanentJob = jobRepository.create(permanentJob);

        // Give some time for the database to register the updates
        Thread.sleep(100);

        jobRepository.reapStaleJobs();

        // Verify stale job is reaped
        Optional<Job> reapedJob = jobRepository.findById(createdStaleJob.id());
        assertTrue(reapedJob.isPresent());
        assertEquals(JobState.QUEUED, reapedJob.get().state());
        assertNull(reapedJob.get().lockedBy());
        assertNull(reapedJob.get().lockedAt());

        // Verify permanent job is not reaped
        Optional<Job> notReapedJob = jobRepository.findById(createdPermanentJob.id());
        assertTrue(notReapedJob.isPresent());
        assertEquals(JobState.RUNNING, notReapedJob.get().state());
        assertEquals(JobLeaseKind.PERMANENT, notReapedJob.get().leaseKind());
    }

    @Test
    void testTransactionalEnqueueing() throws Exception {
        // Scenario 1: Commit
        try (java.sql.Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            Job jobToEnqueue = Job.builder()
                    .kind("transactional-commit-job")
                    .queue("default")
                    .build();

            Job createdJob = jobRepository.create(jobToEnqueue, connection);
            assertNotNull(createdJob.id());
            assertEquals(JobState.QUEUED, createdJob.state());

            // Job should not be visible before commit
            assertFalse(jobRepository.findById(createdJob.id()).isPresent());

            connection.commit();

            // Job should be visible after commit
            Optional<Job> foundJob = jobRepository.findById(createdJob.id());
            assertTrue(foundJob.isPresent());
            assertEquals(createdJob, foundJob.get());
        }

        // Scenario 2: Rollback
        try (java.sql.Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            Job jobToEnqueue = Job.builder()
                    .kind("transactional-rollback-job")
                    .queue("default")
                    .build();

            Job createdJob = jobRepository.create(jobToEnqueue, connection);
            assertNotNull(createdJob.id());
            assertEquals(JobState.QUEUED, createdJob.state());

            // Job should not be visible before rollback
            assertFalse(jobRepository.findById(createdJob.id()).isPresent());

            connection.rollback();

            // Job should not be visible after rollback
            assertFalse(jobRepository.findById(createdJob.id()).isPresent());
        }
    }

    @Test
    void testHookRegistryInvocation() throws Exception {
        final String TEST_JOB_KIND = "hook-test-job";

        // Custom consumers to record invocations
        final List<UUID> enqueuedJobs = new ArrayList<>();
        final List<UUID> dequeuedJobs = new ArrayList<>();
        final List<UUID> completedJobs = new ArrayList<>();
        final List<UUID> failedJobs = new ArrayList<>();
        final List<UUID> discardedJobs = new ArrayList<>();
        final List<UUID> reapedJobs = new ArrayList<>();

        HookRegistry testHookRegistry = new HookRegistry();
        testHookRegistry.registerOnEnqueue(TEST_JOB_KIND, job -> enqueuedJobs.add(job.id()));
        testHookRegistry.registerOnDequeue(TEST_JOB_KIND, job -> dequeuedJobs.add(job.id()));
        testHookRegistry.registerOnComplete(TEST_JOB_KIND, job -> completedJobs.add(job.id()));
        testHookRegistry.registerOnFail(TEST_JOB_KIND, job -> failedJobs.add(job.id()));
        testHookRegistry.registerOnDiscard(TEST_JOB_KIND, job -> discardedJobs.add(job.id()));
        testHookRegistry.registerOnReap(TEST_JOB_KIND, job -> reapedJobs.add(job.id()));

        final PostgresJobRepository localJobRepository = new PostgresJobRepository(dataSource, testHookRegistry);

        // Scenario 1: Enqueue Hook
        Job job1 = Job.builder().kind(TEST_JOB_KIND).queue("q1").build();
        Job createdJob1 = localJobRepository.create(job1);
        assertEquals(1, enqueuedJobs.size());
        assertTrue(enqueuedJobs.contains(createdJob1.id()));

        // Scenario 2: Dequeue Hook (simulated by fetching and locking)
        // Note: fetchAndLockJob will trigger onDequeue internally
        Optional<Job> fetchedJob1 = localJobRepository.fetchAndLockJob(UUID.randomUUID());
        assertTrue(fetchedJob1.isPresent());
        assertEquals(1, dequeuedJobs.size());
        assertTrue(dequeuedJobs.contains(fetchedJob1.get().id()));

        // Scenario 3: Complete Hook
        localJobRepository.markComplete(fetchedJob1.get().id());
        assertEquals(1, completedJobs.size());
        assertTrue(completedJobs.contains(fetchedJob1.get().id()));

        // Scenario 4: Enqueue and Fail Hook
        Job job2 = Job.builder().kind(TEST_JOB_KIND).queue("q2").build();
        Job createdJob2 = localJobRepository.create(job2);
        assertEquals(2, enqueuedJobs.size());
        assertTrue(enqueuedJobs.contains(createdJob2.id()));

        Optional<Job> fetchedJob2 = localJobRepository.fetchAndLockJob(UUID.randomUUID());
        assertTrue(fetchedJob2.isPresent());
        assertEquals(2, dequeuedJobs.size());
        assertTrue(dequeuedJobs.contains(fetchedJob2.get().id()));

        localJobRepository.markFailed(fetchedJob2.get().id(), "Simulated failure");
        assertEquals(1, failedJobs.size());
        assertTrue(failedJobs.contains(fetchedJob2.get().id()));

        // Scenario 5: Discard Hook (after a simulated retry attempt)
        Job job3 = Job.builder().kind(TEST_JOB_KIND).queue("q3").build();
        Job createdJob3 = localJobRepository.create(job3);
        assertEquals(3, enqueuedJobs.size());
        assertTrue(enqueuedJobs.contains(createdJob3.id()));

        // Simulate a job being processed and then discarded (e.g., after max retries)
        localJobRepository.markDiscarded(createdJob3.id());
        assertEquals(1, discardedJobs.size());
        assertTrue(discardedJobs.contains(createdJob3.id()));

        // Scenario 6: Reap Hook
        Job job4 = Job.builder()
                .kind(TEST_JOB_KIND)
                .queue("q4")
                .state(JobState.RUNNING)
                .leaseKind(JobLeaseKind.EXPIRABLE)
                .lockedAt(OffsetDateTime.now().minusHours(2))
                .build();
        Job createdJob4 = localJobRepository.create(job4);
        assertEquals(4, enqueuedJobs.size());
        assertTrue(enqueuedJobs.contains(createdJob4.id()));

        localJobRepository.reapStaleJobs();
        assertEquals(1, reapedJobs.size());
        assertTrue(reapedJobs.contains(createdJob4.id()));

        // Verify final counts for all hooks
        assertEquals(4, enqueuedJobs.size());
        assertEquals(2, dequeuedJobs.size());
        assertEquals(1, completedJobs.size());
        assertEquals(1, failedJobs.size());
        assertEquals(1, discardedJobs.size());
        assertEquals(1, reapedJobs.size());
    }
}
