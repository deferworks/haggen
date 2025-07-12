package co.deferworks.haggen.core;

import java.time.Instant;
import java.util.UUID;

/**
 * Job is the fundamental unit of work in the Haggen framework, everything that
 * gets enqueued, scheduled, or executed is a Job.
 * <p>
 * Each Job has a set of associated metadata can be used to find it in a queue,
 * track its progress and manage its execution. The Job class serves as a lightweight
 * promise that encapsulates the logic for performing the task, this promise is
 * essentially a handle to a row in the Postgres backend and can only be queried
 * or updated transactionally.
 * <p>
 * To ensure job execution is reliable, we use a lease-based system, where each worker when
 * locking a job gets a lease. Leases allow us to continuously monitor jobs and ensure dead workers
 * don't leave jobs in a transient non-recoverable state.
 * <p>
 * Two kinds of leases are allowed, time-based, automatically expiring leases that can be renewed
 * by workers via heart beats and token-based leases which allow non-idempotent jobs to be safely
 * re-enqueued by providing the right token.
 */
public class Job {
    // The unique resource identifier for the job, it is assigned on creation.
    private final UUID id = UUID.randomUUID();

    // The job kind uniquely identifies the type of the job as which worker
    // pool it will be assigned to.
    private final String kind;

    // The metadata field is space used to storing arbitrary metadata about the job.
    private final Byte[] metadata;

    // The queue assigned to the job, this is used to find the job in the.
    private final String queue;

    /**
     * JobState represents the various states a job can be in within the Haggen
     * framework.
     * <p>
     * Each state indicates the current status of a job, allowing for tracking and
     * management of its lifecycle.
     */
    public enum JobState {
        // The job is in the queue and waiting to be processed.
        QUEUED,

        // The job is currently being processed by a worker.
        RUNNING,

        // The job has been successfully completed.
        COMPLETED,

        // The job has failed during processing.
        FAILED,

        // The job has been canceled and will not be processed.
        CANCELLED,

        // The job is currently being retried after a failure.
        RETRYING,

        // The job has been discarded and will not be processed.
        DISCARDED,

        // The job has been scheduled for future execution.
        SCHEDULED,

        // The job is currently paused and will not be processed until resumed.
        PAUSED,
    }

    // The current job state.
    private final JobState state;

    // The job's assigned priority.
    public enum JobPriority {
        URGENT,
        HIGH,
        NORMAL,
        LOW,
    }

    private final JobPriority priority;

    /**
     * Metadata on the job's scheduling and execution.
     */
    public record JobMetadata(Instant createdAt, Instant attemptedAt, Instant scheduledAt) {
    }

    /**
     * JobLeaseKind defines what kind of lease was taken for this job. Expirable leases are time-based and expire
     * at a set timestamp. Tokenized leases are attached to a token, and the lease is freed when the client or worker
     * provides the correct token.
     * <p>
     * All jobs have by default an expirable lease set to 1 hour.
     */
    public enum JobLeaseKind {
        EXPIRABLE,
        PERMANENT,
    }

    /**
     * JobLockKind represents the kind of locks that workers take on a job, row level locks are ideal for workloads
     * that operate within the context of your application. Advisory locks are ideal for workloads that involve
     * external services or are across the boundary of your application. For example, if you model a virtual resource
     * like an AWS resource in your application, you can use an advisory lock to ensure that at most one job updates
     * its metadata in your control plane.
     */
    public enum JobLockKind {
        ROW_LEVEL,
        ADVISORY,
    }

    /**
     * Metadata on the job's lease, the moment it was locked by a worker
     * and the ID of the worker who locked it.
     */
    public record JobLeaseMetadata(JobLeaseKind lease, Instant lockedAt, String lockedBy, JobLockKind lock) {
    }

    /**
     * @param kind:     Job kind, user provided.
     * @param priority: Job priority.
     * @param state:    Job state.
     * @param metadata: Job metadata, arbitrary user provided.
     * @param queue:    Job queue.
     */
    public Job(String kind, JobPriority priority, JobState state, Byte[] metadata, String queue) {
        this.kind = kind;
        this.state = state;
        this.metadata = metadata;
        this.queue = queue;
        this.priority = priority;
    }

    public UUID getId() {
        return id;
    }

    public String getKind() {
        return kind;
    }

    public Byte[] getMetadata() {
        return metadata;
    }

    public String getQueue() {
        return queue;
    }

    public JobState getState() {
        return state;
    }

    public JobPriority getPriority() {
        return priority;
    }


    @Override
    public String toString() {
        return "Job{" +
                "id=" + id +
                ", kind='" + kind + '\'' +
                ", metadata=" + (metadata != null ? metadata.length : 0) +
                ", queue='" + queue + '\'' +
                '}';
    }

}
