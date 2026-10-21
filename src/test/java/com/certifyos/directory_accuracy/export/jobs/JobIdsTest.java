package com.certifyos.directory_accuracy.export.jobs;

import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class JobIdsTest {

    @Test
    void sameInputsGiveTheSameId() {
        UUID first = JobIds.of("select", "org-xyz-candor-2026-10-001", 1);
        UUID again = JobIds.of("select", "org-xyz-candor-2026-10-001", 1);
        Assertions.assertEquals(first, again);
    }

    @Test
    void jobBatchAttemptAndCheckAllChangeTheId() {
        UUID base = JobIds.of("select", "org-xyz-candor-2026-10-001", 1);
        Assertions.assertNotEquals(base, JobIds.of("finish", "org-xyz-candor-2026-10-001", 1));
        Assertions.assertNotEquals(base, JobIds.of("select", "org-xyz-candor-2026-10-002", 1));
        Assertions.assertNotEquals(base, JobIds.of("select", "org-xyz-candor-2026-10-001", 2));
        Assertions.assertNotEquals(
                JobIds.deadline("org-xyz-candor-2026-10-001", 1, 1),
                JobIds.deadline("org-xyz-candor-2026-10-001", 1, 2));
    }
}
