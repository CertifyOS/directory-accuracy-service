package com.certifyos.directory_accuracy.export.jobs;

import org.jboss.logging.MDC;
import org.jobrunr.jobs.context.JobContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class JobLogContextTest {

    @Test
    void putsAndClearsTheThreeKeys() {
        try (JobLogContext ignored = JobLogContext.open("org-a-candor-2026-10-001", null)) {
            JobLogContext.tenant("org-a");
            Assertions.assertEquals("org-a-candor-2026-10-001", MDC.get(JobLogContext.EXPORT_BATCH_ID));
            Assertions.assertEquals("org-a", MDC.get(JobLogContext.TENANT_ID));
            Assertions.assertNull(MDC.get(JobLogContext.JOB_ID), "no JobRunr context outside a job run");
        }
        Assertions.assertNull(MDC.get(JobLogContext.EXPORT_BATCH_ID));
        Assertions.assertNull(MDC.get(JobLogContext.TENANT_ID));
        Assertions.assertNull(MDC.get(JobLogContext.JOB_ID));
    }

    @Test
    void nullJobContextIsTolerated() {
        try (JobLogContext ignored = JobLogContext.open("b", JobContext.Null)) {
            Assertions.assertEquals("b", MDC.get(JobLogContext.EXPORT_BATCH_ID));
            Assertions.assertNull(MDC.get(JobLogContext.JOB_ID));
        }
    }
}
