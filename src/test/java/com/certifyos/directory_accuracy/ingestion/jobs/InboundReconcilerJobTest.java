package com.certifyos.directory_accuracy.ingestion.jobs;

import com.certifyos.directory_accuracy.export.jobs.JobIds;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileState;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class InboundReconcilerJobTest {

    private static final String UPLOAD = "vendor-sftp|to/org-xyz/f.csv|1760520000123456";

    @Test
    void prefixAndCronArePinned() {
        // Alert A3 counts this line in Cloud Logging; a change here is a change to that alert.
        Assertions.assertEquals("INGESTION_RECONCILER_REENQUEUED", InboundReconcilerJob.REENQUEUED_PREFIX);
        Assertions.assertEquals("*/10 * * * *", InboundReconcilerJob.CRON, "every ten minutes (design)");
        Assertions.assertEquals("vendor-ingestion-reconciler", InboundReconcilerJob.RECURRING_ID);
    }

    @Test
    void reconciledIdsAreDistinctPerReconciliationAndFromTheFirstId() {
        Assertions.assertEquals(
                JobIds.of(RequestIngestionJob.NAME, UPLOAD + ":reconciled:1", 1),
                IngestionJobEnqueuer.reconciledId(RequestIngestionJob.NAME, UPLOAD, 1, 1));
        Assertions.assertNotEquals(
                IngestionJobEnqueuer.reconciledId(StageRowsJob.NAME, UPLOAD, 1, 1),
                IngestionJobEnqueuer.reconciledId(StageRowsJob.NAME, UPLOAD, 2, 1));
        Assertions.assertNotEquals(
                JobIds.of(StageRowsJob.NAME, UPLOAD, 1),
                IngestionJobEnqueuer.reconciledId(StageRowsJob.NAME, UPLOAD, 1, 1));
    }

    @Test
    void eachReconcilableStateHasItsJob() {
        Assertions.assertEquals(RequestIngestionJob.class, InboundReconcilerJob.handlerFor(InboundFileState.RECEIVED));
        Assertions.assertEquals(RequestIngestionJob.class, InboundReconcilerJob.handlerFor(InboundFileState.ARCHIVING));
        Assertions.assertEquals(
                IngestionDeadlineCheckJob.class, InboundReconcilerJob.handlerFor(InboundFileState.INGESTION_REQUESTED));
        Assertions.assertEquals(StageRowsJob.class, InboundReconcilerJob.handlerFor(InboundFileState.PARSED));
        Assertions.assertEquals(StageRowsJob.class, InboundReconcilerJob.handlerFor(InboundFileState.STAGING));
        Assertions.assertThrows(
                IllegalStateException.class, () -> InboundReconcilerJob.handlerFor(InboundFileState.STAGED));
    }

    @Test
    void theDeadlineRuleWaitsTheStaleMarginPastTheDeadline() {
        Instant requested = Instant.parse("2026-10-15T09:00:00Z");
        Duration deadline = Duration.ofHours(6);
        Duration stale = Duration.ofMinutes(30);

        Assertions.assertFalse(InboundReconcilerJob.deadlineLost(
                requested, deadline, stale, requested.plus(deadline).plus(stale)));
        Assertions.assertTrue(InboundReconcilerJob.deadlineLost(
                requested, deadline, stale, requested.plus(deadline).plus(stale).plusSeconds(1)));
        Assertions.assertFalse(
                InboundReconcilerJob.deadlineLost(requested, deadline, stale, requested.plusSeconds(60 * 31)));
    }
}
