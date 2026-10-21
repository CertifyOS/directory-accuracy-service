package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.WorkerTestProfile;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Clause;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Operator;
import com.certifyos.directory_accuracy.persistence.Transactions;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.time.YearMonth;
import java.util.List;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The JobRunr gate on the worker role: enqueue once, retry, recurring registration, and the
 * final-failure filter moving a batch to FAILED with its audit event.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(WorkerTestProfile.class)
public class JobRunrGateIT {

    static final SelectionCriteria CRITERIA =
            new SelectionCriteria(List.of(new Clause("data.delegationStatus", Operator.IN, List.of("Direct"))));

    @Inject
    JobRequestScheduler scheduler;

    @Inject
    StorageProvider storage;

    @Inject
    JobEnqueuer enqueuer;

    @Inject
    ExportBatchRepository batches;

    @Inject
    AuditRepository audit;

    @Inject
    Transactions transactions;

    @Test
    void enqueueWithTheSameIdRunsOnce() {
        UUID id = UUID.nameUUIDFromBytes("gate-run-once".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        scheduler.enqueue(id, new GateRunRequest("once"));
        scheduler.enqueue(id, new GateRunRequest("once"));
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .until(() -> storage.getJobById(id).getState() == StateName.SUCCEEDED);
        Assertions.assertEquals(1, GateHandlers.RUNS.get());
    }

    @Test
    void failedJobIsRetriedThenSucceeds() {
        UUID id = UUID.nameUUIDFromBytes("gate-flaky".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        scheduler.enqueue(id, new GateFlakyRequest("flaky"));
        Awaitility.await()
                .atMost(Duration.ofSeconds(90))
                .until(() -> storage.getJobById(id).getState() == StateName.SUCCEEDED);
        Assertions.assertEquals(2, GateHandlers.FLAKY.get(), "one failure, one success");
    }

    @Test
    void bothRecurringJobsAreRegisteredExactlyOnce() {
        Assertions.assertEquals(
                1,
                storage.getRecurringJobs().stream()
                        .filter(job -> job.getId().equals(TickJob.RECURRING_ID))
                        .count());
        Assertions.assertEquals(
                1,
                storage.getRecurringJobs().stream()
                        .filter(job -> job.getId().equals(ReconcilerJob.RECURRING_ID))
                        .count());
        Assertions.assertEquals(
                TickJob.CRON,
                storage.getRecurringJobs().stream()
                        .filter(job -> job.getId().equals(TickJob.RECURRING_ID))
                        .findFirst()
                        .orElseThrow()
                        .getScheduleExpression());
    }

    @Test
    void exhaustedRetriesMarkTheBatchFailedWithItsAuditEvent() {
        ExportBatch batch =
                ExportBatch.scheduled("gate-fail", "candor", YearMonth.of(2026, 10), 1, CRITERIA, Instant.now());
        transactions.run(session -> {
            batches.insert(session, batch);
            return null;
        });

        UUID jobId = enqueuer.select(batch.id(), 1);
        UUID again = enqueuer.select(batch.id(), 1);
        Assertions.assertEquals(jobId, again, "deterministic id, second enqueue is a no-op");

        // The worker profile has no api-layer machine client and CI has no Google credentials, so the
        // select job's first api-layer call throws (which exception depends on the environment); with
        // job-retries=1 in the test profile the job fails, is retried once about 3 s later, fails again
        // and stays FAILED. The filter then moves the batch.
        Awaitility.await()
                .atMost(Duration.ofSeconds(90))
                .until(() -> storage.getJobById(jobId).getState() == StateName.FAILED);
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .until(() -> batches.find(batch.id()).orElseThrow().state() == BatchState.FAILED);

        ExportBatch failed = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(ExportBatch.FailedStep.SELECT, failed.failedStep());
        Assertions.assertTrue(failed.lastError().contains("Exception"), failed.lastError());
        Assertions.assertEquals(2L, failed.version());

        List<AuditEvent> trail = audit.findForBatch(batch.id(), 10);
        AuditEvent event = trail.stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_BATCH_FAILED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals("SELECT", event.detail().get("failedStep"));
        Assertions.assertEquals("JOB_RETRIES_EXHAUSTED", event.detail().get("cause"));
        Assertions.assertEquals(2, event.detail().get("jobAttempts"));
        Assertions.assertEquals(jobId.toString(), event.jobId());
    }

    @Test
    void jobOnARowInAnotherStateExitsAsANoOp() {
        ExportBatch batch =
                ExportBatch.scheduled("gate-noop", "candor", YearMonth.of(2026, 10), 1, CRITERIA, Instant.now());
        transactions.run(session -> {
            batches.insert(session, batch);
            return null;
        });
        // The finish job expects EGRESS_COMPLETED; the row is SCHEDULED, so the job must succeed
        // without doing anything, and the row must be untouched.
        UUID jobId = enqueuer.finish(batch.id(), 1);
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .until(() -> storage.getJobById(jobId).getState() == StateName.SUCCEEDED);
        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.SCHEDULED, after.state());
        Assertions.assertEquals(1L, after.version());
    }
}
