package co.deferworks.haggen.driver;

import co.deferworks.haggen.core.Job;

import java.sql.Connection;

public class PostgresQueue implements Queue {

    private final PostgresJobRepository jobRepository;

    public PostgresQueue(PostgresJobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    @Override
    public Job enqueue(Job job) {
        return jobRepository.create(job);
    }

    @Override
    public Job enqueue(Job job, Connection connection) {
        return jobRepository.create(job, connection);
    }
}