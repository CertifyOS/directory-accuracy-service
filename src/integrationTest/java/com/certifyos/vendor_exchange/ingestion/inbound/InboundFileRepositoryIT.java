package com.certifyos.vendor_exchange.ingestion.inbound;

import com.certifyos.vendor_exchange.ApiTestProfile;
import com.certifyos.vendor_exchange.MongoResource;
import com.certifyos.vendor_exchange.export.batch.ExportBatch;
import com.certifyos.vendor_exchange.export.batch.ExportBatchRepository;
import com.certifyos.vendor_exchange.ingestion.InboundFixtures;
import com.certifyos.vendor_exchange.ingestion.events.ArrivalEvent;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository.Claim;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository.Located;
import com.certifyos.vendor_exchange.persistence.Collections;
import com.certifyos.vendor_exchange.persistence.Ids;
import com.certifyos.vendor_exchange.persistence.Transactions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The inbound registry on {@code vendor_export_batches.inboundFiles[]}: the conditional claim, the
 * guarded element updates, and the reads. Every tenant here starts with {@code inrepo-}; the stale
 * and oldest reads are cross-tenant, so their entries carry timestamps no other test uses.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
public class InboundFileRepositoryIT {

    private static final Logger LOG = Logger.getLogger(InboundFileRepositoryIT.class);
    private static final int WRITERS = 10;
    private static final Instant NOW = Instant.parse("2026-10-15T10:00:00Z");

    @Inject
    InboundFileRepository inbound;

    @Inject
    ExportBatchRepository batches;

    @Inject
    Transactions transactions;

    @Inject
    Collections collections;

    @Inject
    ManagedExecutor executor;

    @Test
    void firstClaimIsReceivedSecondUploadIsDuplicateLogged() {
        ExportBatch batch = InboundFixtures.deliveredBatch(transactions, batches, collections, "inrepo-first");
        InboundFile first = upload("inrepo-first", batch.id(), "1001");
        InboundFile second = upload("inrepo-first", batch.id(), "1002");

        Claim firstClaim = transactions.run(session -> inbound.claim(session, batch.id(), first));
        Claim secondClaim = transactions.run(session -> inbound.claim(session, batch.id(), second));

        Assertions.assertEquals(new Claim(true, InboundFileState.RECEIVED, 1), firstClaim);
        Assertions.assertEquals(
                new Claim(true, InboundFileState.DUPLICATE_LOGGED, 2),
                secondClaim,
                "a repeat upload of the name is recorded, whatever state the caller built it in");
        List<Document> stored = storedEntries(batch.id());
        Assertions.assertEquals(
                List.of(first.uploadId(), second.uploadId()),
                stored.stream().map(doc -> doc.getString("uploadId")).toList());
        Assertions.assertEquals(
                List.of("RECEIVED", "DUPLICATE_LOGGED"),
                stored.stream().map(doc -> doc.getString("state")).toList());
        Located located = inbound.findByUploadId(second.uploadId()).orElseThrow();
        Assertions.assertEquals(batch.id(), located.batch().id());
        Assertions.assertEquals(
                InboundFileState.DUPLICATE_LOGGED, located.entry().state());
        Assertions.assertEquals("1002", located.entry().generation());
    }

    @Test
    void sameUploadIdClaimedTwiceMatchesNothing() {
        ExportBatch batch = InboundFixtures.deliveredBatch(transactions, batches, collections, "inrepo-twice");
        InboundFile entry = upload("inrepo-twice", batch.id(), "2001");

        Claim first = transactions.run(session -> inbound.claim(session, batch.id(), entry));
        Claim again = transactions.run(session -> inbound.claim(session, batch.id(), entry));
        Claim noBatch = transactions.run(session -> inbound.claim(session, "inrepo-nobody-candor-2026-10-001", entry));

        Assertions.assertTrue(first.claimed());
        Assertions.assertEquals(new Claim(false, null, 1), again, "a redelivered event is a no-op");
        Assertions.assertEquals(new Claim(false, null, 0), noBatch);
        List<Document> stored = storedEntries(batch.id());
        Assertions.assertEquals(1, stored.size());
        Assertions.assertEquals("RECEIVED", stored.get(0).getString("state"));
        Assertions.assertTrue(inbound.findByUploadId("nobody|to/x|1").isEmpty());
    }

    /**
     * Ten writers claim the same upload at once. Each task waits on a barrier of ten before it starts
     * its transaction, so the barrier only opens when all ten are running on their own threads at the
     * same time; a serial executor would leave the first task waiting and the barrier times out,
     * failing the test. The losers hit a write conflict, the driver re-runs their body, and the
     * re-run reads the winner's entry and matches nothing.
     */
    @Test
    void claimUnderConcurrentWritersCreatesOneEntry() throws Exception {
        ExportBatch batch = InboundFixtures.deliveredBatch(transactions, batches, collections, "inrepo-race");
        InboundFile entry = upload("inrepo-race", batch.id(), "3001");
        CyclicBarrier start = new CyclicBarrier(WRITERS);
        AtomicInteger bodyRuns = new AtomicInteger();
        Set<String> threads = ConcurrentHashMap.newKeySet();

        List<CompletableFuture<Claim>> writers = new ArrayList<>();
        for (int writer = 0; writer < WRITERS; writer++) {
            writers.add(executor.supplyAsync(() -> {
                threads.add(Thread.currentThread().getName());
                awaitTogether(start);
                return transactions.run(session -> {
                    bodyRuns.incrementAndGet();
                    return inbound.claim(session, batch.id(), entry);
                });
            }));
        }
        CompletableFuture.allOf(writers.toArray(CompletableFuture[]::new)).get(120, TimeUnit.SECONDS);

        List<Claim> claims = new ArrayList<>();
        for (CompletableFuture<Claim> writer : writers) {
            claims.add(writer.get());
        }
        LOG.infof("%d writers on %d threads ran the claim body %d times", WRITERS, threads.size(), bodyRuns.get());
        Assertions.assertEquals(WRITERS, threads.size(), "every writer ran on its own thread");
        Assertions.assertEquals(
                1, claims.stream().filter(Claim::claimed).count(), "exactly one writer claims the upload");
        Assertions.assertTrue(claims.contains(new Claim(true, InboundFileState.RECEIVED, 1)));
        Assertions.assertEquals(1, storedEntries(batch.id()).size());
    }

    @Test
    void transitionIsGuardedByEntryState() {
        InboundFixtures.Seeded seeded = InboundFixtures.deliveredBatchWithEntry(
                transactions, batches, collections, "inrepo-guard", InboundFileState.ARCHIVING);
        String batchId = seeded.batch().id();
        String archiving = seeded.entry().uploadId();
        Document other = entryDoc("inrepo-guard", batchId, InboundFileState.RECEIVED, "4002", NOW.minusSeconds(600));
        collections.batches().updateOne(Filters.eq("_id", batchId), Updates.push("inboundFiles", other));
        String received = other.getString("uploadId");

        boolean crossed = transactions.run(session -> inbound.transition(
                session, batchId, archiving, InboundFileState.RECEIVED, InboundFileState.ARCHIVING, NOW, null));
        Assertions.assertFalse(crossed, "uploadId and state must bind to the same element");

        String ref = Ids.externalRef(batchId, InboundFixtures.GENERATION);
        boolean moved = transactions.run(session -> inbound.transition(
                session,
                batchId,
                archiving,
                InboundFileState.ARCHIVING,
                InboundFileState.INGESTION_REQUESTED,
                NOW,
                Updates.set(InboundFileRepository.entry("externalRef"), ref)));
        boolean stale = transactions.run(session -> inbound.transition(
                session,
                batchId,
                archiving,
                InboundFileState.ARCHIVING,
                InboundFileState.INGESTION_REQUESTED,
                NOW.plusSeconds(1),
                null));
        Assertions.assertTrue(moved);
        Assertions.assertFalse(stale, "a job that read ARCHIVING finds the entry moved and exits as a no-op");

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> transactions.run(session -> inbound.transition(
                        session, batchId, received, InboundFileState.RECEIVED, InboundFileState.STAGED, NOW, null)));

        boolean saved = transactions.run(session -> inbound.updateInState(
                session,
                batchId,
                received,
                InboundFileState.RECEIVED,
                NOW.plusSeconds(5),
                Updates.set(InboundFileRepository.entry("jobId"), "job-received")));
        boolean wrongState = transactions.run(session -> inbound.updateInState(
                session,
                batchId,
                received,
                InboundFileState.ARCHIVING,
                NOW.plusSeconds(6),
                Updates.set(InboundFileRepository.entry("jobId"), "job-wrong")));
        Assertions.assertTrue(saved);
        Assertions.assertFalse(wrongState);

        InboundFile movedEntry = inbound.findByUploadId(archiving).orElseThrow().entry();
        Assertions.assertEquals(InboundFileState.INGESTION_REQUESTED, movedEntry.state());
        Assertions.assertEquals(NOW, movedEntry.updatedAt());
        Assertions.assertEquals(ref, movedEntry.externalRef());
        InboundFile untouched = inbound.findByUploadId(received).orElseThrow().entry();
        Assertions.assertEquals(InboundFileState.RECEIVED, untouched.state(), "the refused edge wrote nothing");
        Assertions.assertNull(untouched.externalRef(), "the transition wrote only its own element");
        Assertions.assertEquals("job-received", untouched.jobId());
        Assertions.assertEquals(NOW.plusSeconds(5), untouched.updatedAt());
    }

    @Test
    void findByExternalRefUsesTheIndex() {
        InboundFixtures.Seeded seeded = InboundFixtures.deliveredBatchWithEntry(
                transactions, batches, collections, "inrepo-ref", InboundFileState.INGESTION_REQUESTED);
        String batchId = seeded.batch().id();
        Document second =
                entryDoc("inrepo-ref", batchId, InboundFileState.INGESTION_REQUESTED, "5002", NOW.minusSeconds(60));
        String secondRef = Ids.externalRef(batchId, "5002");
        second.put("externalRef", secondRef);
        collections.batches().updateOne(Filters.eq("_id", batchId), Updates.push("inboundFiles", second));

        Located located = inbound.findByExternalRef(secondRef).orElseThrow();
        Assertions.assertEquals(batchId, located.batch().id());
        Assertions.assertEquals(second.getString("uploadId"), located.entry().uploadId());
        Assertions.assertEquals(
                seeded.entry().uploadId(),
                inbound.findByExternalRef(seeded.entry().externalRef())
                        .orElseThrow()
                        .entry()
                        .uploadId());
        Assertions.assertTrue(inbound.findByExternalRef("inrepo-nobody|1").isEmpty());

        String plan = winningPlan(InboundFileRepository.byExternalRef(secondRef));
        Assertions.assertTrue(plan.contains("batches_inbound_external_ref"), plan);
    }

    @Test
    void findByUploadIdUsesTheIndex() {
        InboundFixtures.Seeded seeded = InboundFixtures.deliveredBatchWithEntry(
                transactions, batches, collections, "inrepo-upload", InboundFileState.RECEIVED);
        String batchId = seeded.batch().id();
        Document second =
                entryDoc("inrepo-upload", batchId, InboundFileState.DUPLICATE_LOGGED, "8002", NOW.minusSeconds(60));
        String secondUpload = second.getString("uploadId");
        collections.batches().updateOne(Filters.eq("_id", batchId), Updates.push("inboundFiles", second));

        Located located = inbound.findByUploadId(secondUpload).orElseThrow();
        Assertions.assertEquals(batchId, located.batch().id());
        Assertions.assertEquals(
                InboundFileState.DUPLICATE_LOGGED, located.entry().state());
        Assertions.assertEquals(
                InboundFileState.RECEIVED,
                inbound.findByUploadId(seeded.entry().uploadId())
                        .orElseThrow()
                        .entry()
                        .state());

        String plan = winningPlan(InboundFileRepository.byUploadId(secondUpload));
        Assertions.assertTrue(plan.contains("batches_inbound_upload_id"), plan);
    }

    /** The winning plan of a find on the batches collection, as JSON (classic and SBE shapes both name the index). */
    private String winningPlan(Bson filter) {
        Document explain = collections.batches().find(filter).explain();
        return explain.get("queryPlanner", Document.class)
                .get("winningPlan", Document.class)
                .toJson();
    }

    /**
     * Batch A holds an old entry in a state the reconciler does not watch beside a newer stuck one;
     * its array therefore sorts first on {@code inboundFiles.updatedAt}, although its stuck entry is
     * the newest. The read must order by the stuck entries themselves.
     */
    @Test
    void findStaleReturnsOldestFirstWithinLimit() {
        String batchA = batchWith(
                "inrepo-stale-a",
                List.of(
                        new Seed(InboundFileState.STAGED, "6001", day(1)),
                        new Seed(InboundFileState.RECEIVED, "6002", day(5))));
        String batchB = batchWith("inrepo-stale-b", List.of(new Seed(InboundFileState.RECEIVED, "6003", day(2))));
        String batchC = batchWith(
                "inrepo-stale-c",
                List.of(
                        new Seed(InboundFileState.RECEIVED, "6004", day(3)),
                        new Seed(InboundFileState.ARCHIVING, "6005", day(4))));
        batchWith("inrepo-stale-d", List.of(new Seed(InboundFileState.RECEIVED, "6006", day(40))));
        Instant olderThan = day(15);

        List<Located> firstTwo = inbound.findStale(InboundFileState.reconcilable(), olderThan, 2);
        List<Located> all = inbound.findStale(InboundFileState.reconcilable(), olderThan, 10);

        Assertions.assertEquals(List.of("6003", "6004"), generations(firstTwo));
        Assertions.assertEquals(List.of(batchB, batchC), batchIds(firstTwo));
        Assertions.assertEquals(List.of("6003", "6004", "6005", "6002"), generations(all));
        Assertions.assertEquals(List.of(batchB, batchC, batchC, batchA), batchIds(all));
        Assertions.assertEquals(day(2), all.get(0).entry().updatedAt());
        Assertions.assertEquals(
                List.of("6003"),
                generations(inbound.findStale(Set.of(InboundFileState.RECEIVED), day(3), 10)),
                "batch A has a RECEIVED entry and an entry older than day 3, but not one entry that is both");
    }

    @Test
    void countByStateAndOldestUpdatedAtInReadEveryEntry() {
        Map<String, Long> before = inbound.countByState();
        Instant oldest = Instant.parse("1980-03-01T00:00:00Z");
        batchWith(
                "inrepo-count",
                List.of(
                        new Seed(InboundFileState.INGESTION_FAILED, "7001", oldest),
                        new Seed(InboundFileState.INGESTION_FAILED, "7002", oldest.plusSeconds(60)),
                        new Seed(InboundFileState.FAILED, "7003", oldest.plusSeconds(120))));

        Map<String, Long> after = inbound.countByState();

        Assertions.assertEquals(2L, after.get("INGESTION_FAILED") - before.getOrDefault("INGESTION_FAILED", 0L));
        Assertions.assertEquals(1L, after.get("FAILED") - before.getOrDefault("FAILED", 0L));
        Assertions.assertEquals(
                oldest,
                inbound.oldestUpdatedAtIn(InboundFileState.INGESTION_FAILED).orElseThrow());
    }

    private record Seed(InboundFileState state, String generation, Instant updatedAt) {}

    private static Instant day(int dayOfYear) {
        return Instant.parse("1990-01-01T00:00:00Z").plusSeconds((dayOfYear - 1) * 86_400L);
    }

    /** A delivered batch whose {@code inboundFiles} is exactly {@code seeds}, written straight to the collection. */
    private String batchWith(String tenant, List<Seed> seeds) {
        ExportBatch batch = InboundFixtures.deliveredBatch(transactions, batches, collections, tenant);
        List<Document> entries = seeds.stream()
                .map(seed -> entryDoc(tenant, batch.id(), seed.state(), seed.generation(), seed.updatedAt()))
                .toList();
        collections.batches().updateOne(Filters.eq("_id", batch.id()), Updates.set("inboundFiles", entries));
        return batch.id();
    }

    /** The fixture's entry in {@code state}, moved to another generation of the same object and time. */
    private static Document entryDoc(
            String tenant, String exportBatchId, InboundFileState state, String generation, Instant updatedAt) {
        Document doc = InboundFixtures.entryIn(state, tenant, exportBatchId).toDocument();
        doc.put(
                "uploadId",
                Ids.uploadId(
                        InboundFixtures.VENDOR_BUCKET, InboundFixtures.objectName(tenant, exportBatchId), generation));
        doc.put("generation", generation);
        doc.remove("externalRef");
        doc.put("updatedAt", updatedAt);
        return doc;
    }

    /** An upload of the vendor's object for {@code exportBatchId}, as the arrival handler would build it. */
    private static InboundFile upload(String tenant, String exportBatchId, String generation) {
        String objectName = InboundFixtures.objectName(tenant, exportBatchId);
        ArrivalEvent event = new ArrivalEvent(
                InboundFixtures.VENDOR_BUCKET,
                objectName,
                generation,
                "OBJECT_FINALIZE",
                InboundFixtures.ARRIVED_AT.minusSeconds(1),
                null);
        return InboundFile.received(event, InboundNames.parse(objectName).orElseThrow(), InboundFixtures.ARRIVED_AT);
    }

    private List<Document> storedEntries(String exportBatchId) {
        Document doc =
                collections.batches().find(Filters.eq("_id", exportBatchId)).first();
        Assertions.assertNotNull(doc);
        return doc.getList("inboundFiles", Document.class);
    }

    private static void awaitTogether(CyclicBarrier start) {
        try {
            start.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("writer interrupted before the start", interrupted);
        } catch (BrokenBarrierException | TimeoutException notTogether) {
            throw new IllegalStateException("the writers never ran at the same time", notTogether);
        }
    }

    private static List<String> generations(List<Located> located) {
        return located.stream().map(found -> found.entry().generation()).toList();
    }

    private static List<String> batchIds(List<Located> located) {
        return located.stream().map(found -> found.batch().id()).toList();
    }
}
