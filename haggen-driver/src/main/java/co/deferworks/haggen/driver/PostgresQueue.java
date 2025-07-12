package co.deferworks.haggen.driver;

import co.deferworks.haggen.core.Job;

public class PostgresQueue implements Queue {

    private final PostgresJobRepository jobRepository;

    public PostgresQueue(PostgresJobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    @Override
    public Job enqueue(Job job) {
        return jobRepository.create(job);
    }
}