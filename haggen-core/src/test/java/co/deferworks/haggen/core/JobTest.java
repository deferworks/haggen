package co.deferworks.haggen.core;

import co.deferworks.haggen.core.Job.JobPriority;
import co.deferworks.haggen.core.Job.JobState;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class JobTest {
    @Test
    void jobCanBeCreated() {
        var job = Job.builder()
                .id(UUID.randomUUID())
                .kind("send-email")
                .queue("default-queue")
                .metadata("{}")
                .priority(JobPriority.NORMAL)
                .state(JobState.QUEUED)
                .runAt(java.time.OffsetDateTime.now())
                .createdAt(java.time.OffsetDateTime.now())
                .attemptCount(0)
                .lastErrorMessage(null)
                .lastErrorDetails(null)
                .leaseKind(Job.JobLeaseKind.EXPIRABLE)
                .lockedBy(null)
                .lockedAt(null)
                .leaseToken(null)
                .build();
        assertInstanceOf(UUID.class, job.id());
        assertEquals("send-email", job.kind());
        assertEquals(JobPriority.NORMAL, job.priority());
        assertEquals(JobState.QUEUED, job.state());
        assertEquals("default-queue", job.queue());
        assertEquals("{}", job.metadata());
    }
}
