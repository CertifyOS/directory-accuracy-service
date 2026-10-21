package com.certifyos.directory_accuracy.ingestion.jobs;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.config.DirectoryAccuracyConfig;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.ingestion.InboundFixtures;
import com.certifyos.directory_accuracy.ingestion.InboundFixtures.Seeded;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFile;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileRepository;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileState;
import com.certifyos.directory_accuracy.persistence.Collections;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.bson.conversions.Bson;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

/**
 * The inbound reconciler on the api role, where nothing polls, so the jobs it enqueues stay where
 * the test can read them. The stale read is cross-tenant: other tests' entries are found too, so
 * every assertion filters on this class's uploads. Every tenant here starts with {@code inrecon-}.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
class InboundReconcilerJobIT {

    static final int TRAIL_LIMIT = 100;
    static final Duration STALE = Duration.ofMinutes(31);
    static final Duration PAST_DEADLINE = Duration.ofHours(6).plus(STALE);

    @Inject
    InboundReconcilerJob reconciler;

    @Inject
    InboundFileRepository inbound;

    @Inject
    IngestionJobEnqueuer enqueuer;

    @Inject
    ExportBatchRepository batches;

    @Inject
    AuditRepository audit;

    @Inject
    Transactions transactions;

    @Inject
    Collections collections;

    @Inject
    StorageProvider storage;

    @Inject
    Clock clock;

    /** An entry in {@code state} last written {@code age} ago, requested {@code age} ago when it got that far. */
    private Seeded stale(String tenant, InboundFileState state, Duration age) {
        Seeded seeded = InboundFixtures.deliveredBatchWithEntry(transactions, batches, collections, tenant, state);
        Instant at = Instant.now().minus(age).truncatedTo(ChronoUnit.MILLIS);
        Bson updates = Updates.set("inboundFiles.0.updatedAt", at);
        if (seeded.entry().requestedAt() != null) {
            updates = Updates.combine(updates, Updates.set("inboundFiles.0.requestedAt", at));
        }
        collections.batches().updateOne(Filters.eq("_id", seeded.batch().id()), updates);
        return new Seeded(seeded.batch(), entry(seeded));
    }

    private void setJob(Seeded seeded, UUID jobId) {
        collections
                .batches()
                .updateOne(
                        Filters.eq("_id", seeded.batch().id()), Updates.set("inboundFiles.0.jobId", jobId.toString()));
    }

    private InboundFile entry(Seeded seeded) {
        return inbound.findByUploadId(seeded.entry().uploadId()).orElseThrow().entry();
    }

    private Map<String, InboundReconcilerJob.Reenqueued> run(Set<String> uploads) {
        return reconciler.run().stream()
                .filter(done -> uploads.contains(done.uploadId()))
                .collect(Collectors.toMap(InboundReconcilerJob.Reenqueued::uploadId, Function.identity()));
    }

    private List<AuditEvent> reconciledEvents(Seeded seeded) {
        return audit.trailForBatch(seeded.batch().id(), TRAIL_LIMIT).stream()
                .filter(event -> event.type() == AuditEventType.INBOUND_FILE_RECONCILED)
                .toList();
    }

    /** Asserts one re-enqueue: the job under its reconciled id, the entry's counter and job, the event. */
    private void assertReenqueued(
            Seeded seeded,
            InboundReconcilerJob.Reenqueued done,
            String job,
            Class<?> handler,
            int reconciliation,
            long ageMinutes) {
        String uploadId = seeded.entry().uploadId();
        UUID expected = Expected.reconciledId(job, uploadId, reconciliation);
        Assertions.assertEquals(job, done.job(), uploadId);
        Assertions.assertEquals(seeded.entry().state(), done.state());
        Assertions.assertEquals(expected, done.jobId(), "a deterministic id per reconciliation");
        Job stored = storage.getJobById(expected);
        Assertions.assertEquals(StateName.ENQUEUED, stored.getState());
        Assertions.assertEquals(handler.getName(), stored.getJobDetails().getClassName());
        InboundJobRequest request = (InboundJobRequest)
                stored.getJobDetails().getJobParameters().get(0).getObject();
        Assertions.assertEquals(uploadId, request.uploadId(), "the request carries the real upload, not the id key");
        Assertions.assertEquals(seeded.batch().id(), request.exportBatchId());
        Assertions.assertEquals(1, request.attempt());
        InboundFile after = entry(seeded);
        Assertions.assertEquals(seeded.entry().state(), after.state(), "the reconciler never changes a state");
        Assertions.assertEquals(reconciliation, after.reconciliations());
        Assertions.assertEquals(expected.toString(), after.jobId(), "the re-enqueued job now owns the entry");
        AuditEvent event = reconciledEvents(seeded).get(reconciliation - 1);
        Assertions.assertEquals(seeded.entry().state().name(), event.detail().get("state"));
        Assertions.assertEquals(ageMinutes, ((Number) event.detail().get("ageMinutes")).longValue());
        Assertions.assertEquals("RE_ENQUEUED", event.detail().get("action"));
        Assertions.assertEquals(uploadId, event.uploadId());
    }

    @Test
    void staleEntriesAreReenqueuedByState() {
        Seeded received = stale("inrecon-received", InboundFileState.RECEIVED, STALE);
        Seeded archiving = stale("inrecon-archiving", InboundFileState.ARCHIVING, STALE);
        Seeded parsed = stale("inrecon-parsed", InboundFileState.PARSED, STALE);
        Seeded staging = stale("inrecon-staging", InboundFileState.STAGING, STALE);
        Seeded lostCheck = stale("inrecon-lost-check", InboundFileState.INGESTION_REQUESTED, PAST_DEADLINE);
        Seeded waiting = stale("inrecon-waiting", InboundFileState.INGESTION_REQUESTED, STALE);
        Seeded staged = stale("inrecon-staged", InboundFileState.STAGED, STALE);
        List<Seeded> all = List.of(received, archiving, parsed, staging, lostCheck, waiting, staged);
        Set<String> uploads =
                all.stream().map(seeded -> seeded.entry().uploadId()).collect(Collectors.toSet());

        Map<String, InboundReconcilerJob.Reenqueued> done = run(uploads);

        Assertions.assertEquals(5, done.size(), "five jobs: " + done.keySet());
        assertReenqueued(
                received,
                done.get(received.entry().uploadId()),
                RequestIngestionJob.NAME,
                RequestIngestionJob.class,
                1,
                31);
        assertReenqueued(
                archiving,
                done.get(archiving.entry().uploadId()),
                RequestIngestionJob.NAME,
                RequestIngestionJob.class,
                1,
                31);
        assertReenqueued(parsed, done.get(parsed.entry().uploadId()), StageRowsJob.NAME, StageRowsJob.class, 1, 31);
        assertReenqueued(staging, done.get(staging.entry().uploadId()), StageRowsJob.NAME, StageRowsJob.class, 1, 31);
        assertReenqueued(
                lostCheck,
                done.get(lostCheck.entry().uploadId()),
                IngestionDeadlineCheckJob.NAME,
                IngestionDeadlineCheckJob.class,
                1,
                PAST_DEADLINE.toMinutes());
        for (Seeded untouched : List.of(waiting, staged)) {
            Assertions.assertEquals(0, entry(untouched).reconciliations());
            Assertions.assertEquals(
                    untouched.entry().updatedAt(), entry(untouched).updatedAt());
            Assertions.assertEquals(List.of(), reconciledEvents(untouched));
        }
        Assertions.assertEquals(
                5,
                all.stream().mapToInt(seeded -> reconciledEvents(seeded).size()).sum(),
                "five INBOUND_FILE_RECONCILED");

        Assertions.assertEquals(Map.of(), run(uploads), "a second pass right after finds nothing stale");
        Assertions.assertEquals(
                5,
                all.stream().mapToInt(seeded -> reconciledEvents(seeded).size()).sum());
    }

    @Test
    void entryWithALiveJobIsNotReenqueued() {
        Seeded staging = stale("inrecon-live", InboundFileState.STAGING, STALE);
        UUID live = enqueuer.stageRows(staging.batch().id(), staging.entry().uploadId(), 1);
        setJob(staging, live);
        Seeded checked = stale("inrecon-live-check", InboundFileState.INGESTION_REQUESTED, PAST_DEADLINE);
        UUID check = enqueuer.deadlineCheck(
                checked.batch().id(),
                checked.entry().uploadId(),
                1,
                Instant.now().plus(Duration.ofHours(1)));
        setJob(checked, check);

        Map<String, InboundReconcilerJob.Reenqueued> done =
                run(Set.of(staging.entry().uploadId(), checked.entry().uploadId()));

        Assertions.assertEquals(Map.of(), done, "a job still waiting to run owns its entry");
        Assertions.assertEquals(0, entry(staging).reconciliations());
        Assertions.assertEquals(live.toString(), entry(staging).jobId());
        Assertions.assertEquals(List.of(), reconciledEvents(staging));
        Assertions.assertEquals(List.of(), reconciledEvents(checked));
    }

    @Test
    void pendingFirstJobThatWasNeverRecordedBlocks() {
        // The hand-over enqueued the stage job but its job id write failed: the entry still names
        // another job (the fixture's), while the stage job itself waits to run.
        Seeded staging = stale("inrecon-unrecorded", InboundFileState.STAGING, STALE);
        UUID pending = enqueuer.stageRows(staging.batch().id(), staging.entry().uploadId(), 1);
        Assertions.assertNotEquals(pending.toString(), entry(staging).jobId());

        Map<String, InboundReconcilerJob.Reenqueued> done =
                run(Set.of(staging.entry().uploadId()));

        Assertions.assertEquals(Map.of(), done, "the state's first job still owns the entry");
        Assertions.assertEquals(0, entry(staging).reconciliations());
        Assertions.assertEquals(List.of(), reconciledEvents(staging));
    }

    @Test
    void liveJobOfAnotherKindDoesNotBlock() {
        // A PARSED entry whose job is still the deadline check: the stage-rows hand-over never happened.
        Seeded parsed = stale("inrecon-other-kind", InboundFileState.PARSED, STALE);
        UUID check = enqueuer.deadlineCheck(
                parsed.batch().id(), parsed.entry().uploadId(), 1, Instant.now().plus(Duration.ofHours(1)));
        setJob(parsed, check);
        Seeded withJob = new Seeded(parsed.batch(), entry(parsed));

        Map<String, InboundReconcilerJob.Reenqueued> done =
                run(Set.of(parsed.entry().uploadId()));

        assertReenqueued(withJob, done.get(parsed.entry().uploadId()), StageRowsJob.NAME, StageRowsJob.class, 1, 31);
    }

    @Test
    void aReconciledJobThatEndedIsReplacedUnderTheNextNumber() {
        Seeded received = stale("inrecon-again", InboundFileState.RECEIVED, STALE);
        String uploadId = received.entry().uploadId();
        UUID first = run(Set.of(uploadId)).get(uploadId).jobId();
        enqueuer.delete(first);
        Instant later = Instant.now().minus(STALE).truncatedTo(ChronoUnit.MILLIS);
        collections
                .batches()
                .updateOne(Filters.eq("_id", received.batch().id()), Updates.set("inboundFiles.0.updatedAt", later));
        Seeded asRead = new Seeded(received.batch(), entry(received));

        Map<String, InboundReconcilerJob.Reenqueued> done = run(Set.of(uploadId));

        assertReenqueued(asRead, done.get(uploadId), RequestIngestionJob.NAME, RequestIngestionJob.class, 2, 31);
        Assertions.assertEquals(StateName.DELETED, storage.getJobById(first).getState());
    }

    @Test
    void disabledServiceDoesNothing() {
        Seeded received = stale("inrecon-disabled", InboundFileState.RECEIVED, STALE);
        DirectoryAccuracyConfig disabled = Mockito.mock(DirectoryAccuracyConfig.class, Answers.RETURNS_DEEP_STUBS);
        Mockito.when(disabled.enabled()).thenReturn(false);
        InboundReconcilerJob off = new InboundReconcilerJob(disabled, inbound, audit, transactions, enqueuer, clock);

        Assertions.assertEquals(List.of(), off.run());
        Assertions.assertEquals(0, entry(received).reconciliations());
    }

    /** The reconciled ids the tests expect, spelled out rather than taken from the code under test. */
    static final class Expected {
        private Expected() {}

        static UUID reconciledId(String job, String uploadId, int reconciliation) {
            return com.certifyos.directory_accuracy.export.jobs.JobIds.of(
                    job, uploadId + ":reconciled:" + reconciliation, 1);
        }
    }
}
