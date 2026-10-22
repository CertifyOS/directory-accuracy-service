package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.persistence.Transactions;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Instant;
import java.time.YearMonth;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** The reconciler re-enqueues by state; on the api role the jobs stay ENQUEUED where the test can see them. */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
public class ReconcilerJobIT {

    static final String FILTER_ID = "flt-direct";
    static final YearMonth PERIOD = YearMonth.of(2026, 10);

    @Inject
    ReconcilerJob reconciler;

    @Inject
    ExportBatchRepository batches;

    @Inject
    Transactions transactions;

    @Inject
    StorageProvider storage;

    private ExportBatch staleIn(String tenantId, BatchState state) {
        Instant old = Instant.now().minusSeconds(3600);
        ExportBatch batch = ExportBatch.scheduled(tenantId, "candor", PERIOD, 1, FILTER_ID, old);
        transactions.run(session -> {
            batches.insert(session, batch);
            BatchState current = BatchState.SCHEDULED;
            for (BatchState next : path(state)) {
                batches.transition(session, batch.id(), current, next, old, null);
                current = next;
            }
            return null;
        });
        return batch;
    }

    private static List<BatchState> path(BatchState target) {
        return switch (target) {
            case SCHEDULED -> List.of();
            case EGRESS_REQUESTED -> List.of(BatchState.EGRESS_REQUESTED);
            case EGRESS_COMPLETED -> List.of(BatchState.EGRESS_REQUESTED, BatchState.EGRESS_COMPLETED);
            default -> throw new IllegalArgumentException(target.name());
        };
    }

    @Test
    void staleRowsGetTheJobTheirStateNamesAndEgressRequestedGetsNothing() {
        ExportBatch scheduled = staleIn("recon-scheduled", BatchState.SCHEDULED);
        ExportBatch requested = staleIn("recon-requested", BatchState.EGRESS_REQUESTED);
        ExportBatch completed = staleIn("recon-completed", BatchState.EGRESS_COMPLETED);
        ExportBatch fresh = ExportBatch.scheduled("recon-fresh", "candor", PERIOD, 1, FILTER_ID, Instant.now());
        transactions.run(session -> {
            batches.insert(session, fresh);
            return null;
        });

        Map<String, ReconcilerJob.Reenqueued> byBatch = reconciler.run().stream()
                .filter(entry -> entry.exportBatchId().startsWith("recon-"))
                .collect(Collectors.toMap(ReconcilerJob.Reenqueued::exportBatchId, Function.identity()));

        Assertions.assertEquals(
                RequestEgressJob.NAME, byBatch.get(scheduled.id()).job());
        Assertions.assertEquals(FinishJob.NAME, byBatch.get(completed.id()).job());
        Assertions.assertFalse(byBatch.containsKey(requested.id()), "EGRESS_REQUESTED belongs to the deadline check");
        Assertions.assertFalse(byBatch.containsKey(fresh.id()), "a fresh row is not stale");
        for (ReconcilerJob.Reenqueued entry : byBatch.values()) {
            Assertions.assertEquals(JobIds.of(entry.job(), entry.exportBatchId(), 1), entry.jobId());
            Assertions.assertEquals(
                    StateName.ENQUEUED, storage.getJobById(entry.jobId()).getState());
        }
    }

    @Test
    void aSecondPassReusesTheSameJobIds() {
        ExportBatch batch = staleIn("recon-twice", BatchState.SCHEDULED);

        ReconcilerJob.Reenqueued first = reconciler.run().stream()
                .filter(entry -> entry.exportBatchId().equals(batch.id()))
                .findFirst()
                .orElseThrow();
        ReconcilerJob.Reenqueued second = reconciler.run().stream()
                .filter(entry -> entry.exportBatchId().equals(batch.id()))
                .findFirst()
                .orElseThrow();

        Assertions.assertEquals(first.jobId(), second.jobId(), "deterministic id, the second enqueue is a no-op");
        Assertions.assertEquals(
                StateName.ENQUEUED, storage.getJobById(first.jobId()).getState());
    }
}
