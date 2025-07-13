package co.deferworks.haggen.driver;

import co.deferworks.haggen.core.Job;

import java.util.Optional;
import java.util.UUID;

/**
 * The JobRepository interface defines the contract for all data access operations
 * related to jobs. It provides a clear separation between the business logic and the
 * data access layer, ensuring that the rest of the application is decoupled from
 * the underlying database implementation.
 * <p>
 * This interface is designed to be implemented by a concrete class, such as
 * PostgresJobRepository, which will handle the specifics of interacting with the
 * PostgreSQL database.
 */
public interface JobRepository {

    /**
     * Enqueues a new job by inserting it into the database.
     *
     * @param job The job to be enqueued.
     * @return The newly created job, including its generated ID and other default values.
     */
    Job create(Job job);

    /**
     * Enqueues a new job by inserting it into the database within an existing transaction.
     *
     * @param job The job to be enqueued.
     * @param connection The existing JDBC connection to use for the transaction.
     * @return The newly created job, including its generated ID and other default values.
     */
    Job create(Job job, java.sql.Connection connection);

    /**
     * Retrieves a job by its unique ID.
     *
     * @param id The ID of the job to retrieve.
     * @return An Optional containing the job if found, or an empty Optional otherwise.
     */
    Optional<Job> findById(UUID id);

    /**
     * Fetches and locks the next available job from the queue, making it ready for processing.
     * This method uses a SELECT ... FOR UPDATE SKIP LOCKED query to ensure that multiple
     * workers can safely and efficiently poll for jobs without conflicts.
     *
     * @param workerId The ID of the worker that is attempting to lock the job.
     * @return An Optional containing the locked job if one was available, or an empty Optional otherwise.
     */
    Optional<Job> fetchAndLockJob(UUID workerId);

    /**
     * Marks a job as completed.
     *
     * @param jobId The ID of the job to mark as completed.
     */
    void markComplete(UUID jobId);

    /**
     * Marks a job as failed.
     *
     * @param jobId The ID of the job to mark as failed.
     * @param errorMessage A descriptive message explaining the cause of the failure.
     */
    void markFailed(UUID jobId, String errorMessage);

    /**
     * Resets stale jobs with expired leases back to the PENDING state, so they can be picked up again.
     * This method is a critical part of the fault-tolerance mechanism, ensuring that jobs are not lost
     * if a worker fails unexpectedly.
     */
    void reapStaleJobs();

    /**
     * Marks a job as discarded.
     *
     * @param jobId The ID of the job to mark as discarded.
     */
    void markDiscarded(UUID jobId);

    /**
     * Marks a job as retrying, increments its attempt count, and sets a new run_at time for backoff.
     *
     * @param jobId The ID of the job to mark as retrying.
     * @param errorMessage A descriptive message explaining the cause of the failure.
     * @param runAt The new run_at timestamp for the job (for backoff).
     */
    void markRetrying(UUID jobId, String errorMessage, java.time.OffsetDateTime runAt);

    /**
     * Renews the lease on a job, extending its `locked_at` timestamp to prevent it from being reaped.
     *
     * @param jobId The ID of the job to renew the lease for.
     * @param workerId The ID of the worker holding the lease.
     */
    void renewLease(UUID jobId, UUID workerId);
}
