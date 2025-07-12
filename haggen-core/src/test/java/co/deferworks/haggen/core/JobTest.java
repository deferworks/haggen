package co.deferworks.haggen.core;

import co.deferworks.haggen.core.Job.JobPriority;
import co.deferworks.haggen.core.Job.JobState;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class JobTest {
    @Test
    void jobCanBeCreated() {
        var job = new Job("send-email", JobPriority.NORMAL, JobState.QUEUED, null, "default-queue");
        assertInstanceOf(UUID.class, job.getId());
        assertEquals("send-email", job.getKind());
        assertEquals(JobPriority.NORMAL, job.getPriority());
        assertEquals(JobState.QUEUED, job.getState());
        assertEquals("default-queue", job.getQueue());
        assertNull(job.getMetadata());
    }
}
