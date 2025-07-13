package co.deferworks.haggen.core;

import java.time.OffsetDateTime;
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
public record Job(
        UUID id,
        String kind,
        String queue,
        String metadata, // Stored as JSONB in the database
        JobPriority priority,
        JobState state,
        OffsetDateTime runAt,
        OffsetDateTime createdAt,
        int attemptCount,
        String lastErrorMessage,
        String lastErrorDetails,
        JobLeaseKind leaseKind,
        UUID lockedBy,
        OffsetDateTime lockedAt,
        UUID leaseToken
) {

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private UUID id;
        private String kind;
        private String queue;
        private String metadata = "{}";
        private JobPriority priority = JobPriority.NORMAL;
        private JobState state = JobState.QUEUED;
        private OffsetDateTime runAt = OffsetDateTime.now();
        private OffsetDateTime createdAt = OffsetDateTime.now();
        private int attemptCount = 0;
        private String lastErrorMessage;
        private String lastErrorDetails;
        private JobLeaseKind leaseKind = JobLeaseKind.EXPIRABLE;
        private UUID lockedBy;
        private OffsetDateTime lockedAt;
        private UUID leaseToken;

        private Builder() {
        }

        public Builder id(UUID id) {
            this.id = id;
            return this;
        }

        public Builder kind(String kind) {
            this.kind = kind;
            return this;
        }

        public Builder queue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder metadata(String metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder priority(JobPriority priority) {
            this.priority = priority;
            return this;
        }

        public Builder state(JobState state) {
            this.state = state;
            return this;
        }

        public Builder runAt(OffsetDateTime runAt) {
            this.runAt = runAt;
            return this;
        }

        public Builder createdAt(OffsetDateTime createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public Builder attemptCount(int attemptCount) {
            this.attemptCount = attemptCount;
            return this;
        }

        public Builder lastErrorMessage(String lastErrorMessage) {
            this.lastErrorMessage = lastErrorMessage;
            return this;
        }

        public Builder lastErrorDetails(String lastErrorDetails) {
            this.lastErrorDetails = lastErrorDetails;
            return this;
        }

        public Builder leaseKind(JobLeaseKind leaseKind) {
            this.leaseKind = leaseKind;
            return this;
        }

        public Builder lockedBy(UUID lockedBy) {
            this.lockedBy = lockedBy;
            return this;
        }

        public Builder lockedAt(OffsetDateTime lockedAt) {
            this.lockedAt = lockedAt;
            return this;
        }

        public Builder leaseToken(UUID leaseToken) {
            this.leaseToken = leaseToken;
            return this;
        }

        public Job build() {
            return new Job(id, kind, queue, metadata, priority, state, runAt, createdAt, attemptCount, lastErrorMessage, lastErrorDetails, leaseKind, lockedBy, lockedAt, leaseToken);
        }
    }

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

        // The job has been discarded and will not be processed.
        DISCARDED,

        // The job has been scheduled for future execution.
        SCHEDULED,

        // The job is currently paused and will not be processed until resumed.
        PAUSED,
    }

    /**
     * The job's assigned priority. This maps to an integer in the database.
     */
    public enum JobPriority {
        URGENT(0),
        HIGH(1),
        NORMAL(2),
        LOW(3);

        private final int value;

        JobPriority(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
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

    @Override
    public String toString() {
        return "job.id=" + id +
                " job.kind=" + kind +
                " job.queue=" + queue +
                " job.state=" + state +
                " job.priority=" + priority +
                " job.run_at=" + runAt
                ;
    }
}
