package co.deferworks.haggen.driver;

import co.deferworks.haggen.core.Job;
import co.deferworks.haggen.core.Job.JobLeaseKind;
import co.deferworks.haggen.core.Job.JobPriority;
import co.deferworks.haggen.core.Job.JobState;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import db.migration.DatabaseMigrations;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.OffsetDateTime;
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
    private static final Logger log = LoggerFactory.getLogger(PostgresJobRepositoryTest.class);

    private HikariDataSource dataSource;
    private PostgresJobRepository jobRepository;

    @BeforeEach
    void setUp() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        dataSource = new HikariDataSource(config);

        // Apply Flyway migrations
        Flyway flyway = Flyway.configure(DatabaseMigrations.class.getClassLoader()).dataSource(dataSource)
                .locations("classpath:db/migration").load();
        flyway.migrate();

        jobRepository = new PostgresJobRepository(dataSource);
    }

    @AfterEach
    void tearDown() {
        if (dataSource != null) {
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
        log.info(reapedJob.get().toString());
        assertTrue(reapedJob.isPresent());
        assertEquals(JobState.RUNNING, reapedJob.get().state());
        assertNull(reapedJob.get().lockedBy());
        assertNotNull(reapedJob.get().lockedAt());

        // Verify permanent job is not reaped
        Optional<Job> notReapedJob = jobRepository.findById(createdPermanentJob.id());
        log.info(notReapedJob.get().toString());
        assertTrue(notReapedJob.isPresent());
        assertEquals(JobState.QUEUED, notReapedJob.get().state());
        assertEquals(JobLeaseKind.PERMANENT, notReapedJob.get().leaseKind());
    }
}
