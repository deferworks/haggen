package co.deferworks.haggen.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class JobExecutionContextTest {
    @Test
    void contextDoesExits() {
        assertNotNull(JobExecutionContext.class);
    }
}
