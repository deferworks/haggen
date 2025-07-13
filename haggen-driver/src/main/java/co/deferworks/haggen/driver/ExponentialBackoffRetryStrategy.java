package co.deferworks.haggen.driver;

import co.deferworks.haggen.core.Job;

import java.time.OffsetDateTime;

public class ExponentialBackoffRetryStrategy implements RetryStrategy {

    @Override
    public OffsetDateTime nextRetryTime(Job job) {
        return OffsetDateTime.now().plusSeconds((long) Math.pow(2, job.attemptCount()));
    }
}
