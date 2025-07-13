package co.deferworks.haggen.driver;

import co.deferworks.haggen.core.Job;

import java.time.OffsetDateTime;

@FunctionalInterface
public interface RetryStrategy {
    OffsetDateTime nextRetryTime(Job job);
}
