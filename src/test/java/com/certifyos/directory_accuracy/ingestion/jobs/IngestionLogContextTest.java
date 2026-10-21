package com.certifyos.directory_accuracy.ingestion.jobs;

import com.certifyos.directory_accuracy.export.jobs.JobLogContext;
import java.util.UUID;
import org.jboss.logging.MDC;
import org.jobrunr.jobs.context.JobContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IngestionLogContextTest {

    static final UUID JOB = UUID.fromString("6b1f3c4e-0d2a-4f6e-9a8b-1c2d3e4f5a6b");

    /** A JobRunr context with an id, as a running job would have. */
    static JobContext running() {
        return new JobContext() {
            @Override
            public UUID getJobId() {
                return JOB;
            }
        };
    }

    @Test
    void putsAndClearsTheBatchUploadTenantAndJobKeys() {
        try (IngestionLogContext ignored =
                IngestionLogContext.open("org-a-candor-2026-10-001", "vendor-sftp|to/org-a/f.csv|17", running())) {
            IngestionLogContext.tenant("org-a");
            Assertions.assertEquals("org-a-candor-2026-10-001", MDC.get(IngestionLogContext.EXPORT_BATCH_ID));
            Assertions.assertEquals("vendor-sftp|to/org-a/f.csv|17", MDC.get(IngestionLogContext.UPLOAD_ID));
            Assertions.assertEquals("org-a", MDC.get(IngestionLogContext.TENANT_ID));
            Assertions.assertEquals(JOB.toString(), MDC.get(IngestionLogContext.JOB_ID));
        }
        Assertions.assertNull(MDC.get(IngestionLogContext.EXPORT_BATCH_ID));
        Assertions.assertNull(MDC.get(IngestionLogContext.UPLOAD_ID));
        Assertions.assertNull(MDC.get(IngestionLogContext.TENANT_ID));
        Assertions.assertNull(MDC.get(IngestionLogContext.JOB_ID));
    }

    @Test
    void sharesTheExportLaneKeyNames() {
        Assertions.assertEquals(JobLogContext.EXPORT_BATCH_ID, IngestionLogContext.EXPORT_BATCH_ID);
        Assertions.assertEquals(JobLogContext.TENANT_ID, IngestionLogContext.TENANT_ID);
        Assertions.assertEquals(JobLogContext.JOB_ID, IngestionLogContext.JOB_ID);
    }

    @Test
    void releaseContextPutsAndClearsTenantReleaseAndJob() {
        try (IngestionLogContext ignored = IngestionLogContext.openRelease("org-a", "rel-1", running())) {
            Assertions.assertEquals("org-a", MDC.get(IngestionLogContext.TENANT_ID));
            Assertions.assertEquals("rel-1", MDC.get(IngestionLogContext.RELEASE_ID));
            Assertions.assertEquals(JOB.toString(), MDC.get(IngestionLogContext.JOB_ID));
            Assertions.assertNull(MDC.get(IngestionLogContext.EXPORT_BATCH_ID));
        }
        Assertions.assertNull(MDC.get(IngestionLogContext.TENANT_ID));
        Assertions.assertNull(MDC.get(IngestionLogContext.RELEASE_ID));
        Assertions.assertNull(MDC.get(IngestionLogContext.JOB_ID));
    }

    @Test
    void missingJobContextLeavesNoJobKey() {
        try (IngestionLogContext ignored = IngestionLogContext.open("b", "u", null)) {
            Assertions.assertNull(MDC.get(IngestionLogContext.JOB_ID), "no JobRunr context outside a job run");
        }
        try (IngestionLogContext ignored = IngestionLogContext.open("b", "u", JobContext.Null)) {
            Assertions.assertEquals("b", MDC.get(IngestionLogContext.EXPORT_BATCH_ID));
            Assertions.assertNull(MDC.get(IngestionLogContext.JOB_ID));
        }
    }
}
