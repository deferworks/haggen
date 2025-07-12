package co.deferworks.haggen.driver;

import co.deferworks.haggen.core.Job;

import javax.sql.DataSource;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

/**
 * The PostgresJobRepository is a concrete implementation of the JobRepository
 * interface, tailored for a PostgreSQL database. It handles all the specifics of
 * SQL queries, transactions, and data mapping to and from the Job record.
 * <p>
 * This class uses a javax.sql.DataSource for managing database connections, which
 * is a standard and efficient way to handle connection pooling.
 */
public class PostgresJobRepository implements JobRepository {

    private final DataSource dataSource;

    public PostgresJobRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private static final String CREATE_JOB_SQL = """
            INSERT INTO jobs (kind, queue, metadata, priority, state, lease_kind, locked_by, locked_at, lease_token)
            VALUES (?, ?, CAST(? AS JSONB), ?, ?, ?, ?, ?, ?)
            RETURNING id, kind, queue, metadata, priority, state, run_at, created_at, attempt_count, last_error_message, last_error_details, lease_kind, locked_by, locked_at, lease_token;
            """;

    @Override
    public Job create(Job job) {
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement(CREATE_JOB_SQL)) {

            statement.setString(1, job.kind());
            statement.setString(2, job.queue());
            statement.setString(3, job.metadata());
            statement.setInt(4, job.priority().getValue());
            statement.setString(5, job.state().name());
            statement.setString(6, job.leaseKind().name());
            statement.setObject(7, job.lockedBy());
            statement.setObject(8, job.lockedAt());
            statement.setObject(9, job.leaseToken());

            var resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return mapRowToJob(resultSet);
            } else {
                throw new RuntimeException("Failed to create job, no rows returned.");
            }
        } catch (java.sql.SQLException e) {
            throw new RuntimeException("Error creating job", e);
        }
    }

    private static final String FIND_BY_ID_SQL = """
            SELECT id, kind, queue, metadata, priority, state, run_at, created_at, attempt_count, last_error_message, last_error_details, lease_kind, locked_by, locked_at, lease_token
            FROM jobs
            WHERE id = ?;
            """;

    @Override
    public Optional<Job> findById(UUID id) {
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement(FIND_BY_ID_SQL)) {

            statement.setObject(1, id);

            var resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return Optional.of(mapRowToJob(resultSet));
            } else {
                return Optional.empty();
            }
        } catch (java.sql.SQLException e) {
            throw new RuntimeException("Error finding job by id", e);
        }
    }

    private static final String FETCH_AND_LOCK_JOB_SQL = """
            UPDATE jobs
            SET state = 'RUNNING', locked_by = ?, locked_at = NOW()
            WHERE id = (
                SELECT id
                FROM jobs
                WHERE state = 'QUEUED' AND run_at <= NOW()
                ORDER BY priority DESC, run_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, kind, queue, metadata, priority, state, run_at, created_at, attempt_count, last_error_message, last_error_details, lease_kind, locked_by, locked_at, lease_token;
            """;

    @Override
    public Optional<Job> fetchAndLockJob(UUID workerId) {
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement(FETCH_AND_LOCK_JOB_SQL)) {

            statement.setObject(1, workerId);

            var resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return Optional.of(mapRowToJob(resultSet));
            } else {
                return Optional.empty();
            }
        } catch (java.sql.SQLException e) {
            throw new RuntimeException("Error fetching and locking job", e);
        }
    }

    private static final String MARK_COMPLETE_SQL = """
            UPDATE jobs
            SET state = 'COMPLETED'
            WHERE id = ?;
            """;

    @Override
    public void markComplete(UUID jobId) {
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement(MARK_COMPLETE_SQL)) {

            statement.setObject(1, jobId);
            statement.executeUpdate();
        } catch (java.sql.SQLException e) {
            throw new RuntimeException("Error marking job as complete", e);
        }
    }

    private static final String MARK_FAILED_SQL = """
            UPDATE jobs
            SET state = 'FAILED', last_error_message = ?
            WHERE id = ?;
            """;

    @Override
    public void markFailed(UUID jobId, String errorMessage) {
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement(MARK_FAILED_SQL)) {

            statement.setString(1, errorMessage);
            statement.setObject(2, jobId);
            statement.executeUpdate();
        } catch (java.sql.SQLException e) {
            throw new RuntimeException("Error marking job as failed", e);
        }
    }

    private static final String REAP_STALE_JOBS_SQL = """
            UPDATE jobs
            SET state = 'QUEUED', locked_by = NULL, locked_at = NULL
            WHERE state = 'RUNNING' AND lease_kind = 'PERMANENT';
            """;

    @Override
    public void reapStaleJobs() {
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement(REAP_STALE_JOBS_SQL)) {

            statement.executeUpdate();
        } catch (java.sql.SQLException e) {
            throw new RuntimeException("Error reaping stale jobs", e);
        }
    }

    private Job mapRowToJob(java.sql.ResultSet rs) throws java.sql.SQLException {
        return Job.builder()
                .id(rs.getObject("id", UUID.class))
                .kind(rs.getString("kind"))
                .queue(rs.getString("queue"))
                .metadata(rs.getString("metadata"))
                .priority(Job.JobPriority.values()[rs.getInt("priority")])
                .state(Job.JobState.valueOf(rs.getString("state")))
                .runAt(rs.getObject("run_at", OffsetDateTime.class))
                .createdAt(rs.getObject("created_at", OffsetDateTime.class))
                .attemptCount(rs.getInt("attempt_count"))
                .lastErrorMessage(rs.getString("last_error_message"))
                .lastErrorDetails(rs.getString("last_error_details"))
                .leaseKind(Job.JobLeaseKind.valueOf(rs.getString("lease_kind")))
                .lockedBy(rs.getObject("locked_by", UUID.class))
                .lockedAt(rs.getObject("locked_at", OffsetDateTime.class))
                .leaseToken(rs.getObject("lease_token", UUID.class))
                .build();
    }
}
