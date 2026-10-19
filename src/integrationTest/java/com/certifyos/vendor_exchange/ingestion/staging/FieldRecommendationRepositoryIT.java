package com.certifyos.vendor_exchange.ingestion.staging;

import com.certifyos.vendor_exchange.ApiTestProfile;
import com.certifyos.vendor_exchange.MongoResource;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation.GoldenRecordUpdate;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation.HistoryEntry;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation.Location;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation.Practitioner;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation.SourceFile;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendationRepository.ListQuery;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendationRepository.Upserted;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The {@code field_recommendations} repository: the idempotent staging upsert, the keyset list on
 * the partial index, the counts and the guarded decision. Every tenant here starts with
 * {@code frrepo-}; the system-wide count reads compare before and after.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
public class FieldRecommendationRepositoryIT {

    private static final Instant STAGED = Instant.parse("2026-10-15T09:15:10Z");
    private static final String SHEET = "Physicians Directory Accuracy";
    private static final Pattern INDEX_NAME = Pattern.compile("\"indexName\": \"([^\"]+)\"");

    @Inject
    FieldRecommendationRepository repository;

    @Inject
    Transactions transactions;

    @Inject
    Collections collections;

    @Test
    void upsertTwiceInsertsOnce() {
        String tenant = "frrepo-upsert";
        List<FieldRecommendation> docs = List.of(
                recommendation(tenant, batchId(tenant, 10), "cert-1", "1000000001", "loc-1", "phone", STAGED),
                recommendation(tenant, batchId(tenant, 10), "cert-1", "1000000001", null, "first_name", STAGED));

        Upserted first = repository.upsertAll(docs);
        boolean decided = transactions.run(session -> repository.decide(
                session,
                tenant,
                docs.get(0).id(),
                1L,
                ReviewStatus.open(),
                ReviewStatus.APPROVED,
                history("APPROVE", STAGED.plusSeconds(5)),
                STAGED.plusSeconds(5)));
        List<FieldRecommendation> restaged = docs.stream()
                .map(doc -> withUpdatedAt(doc, STAGED.plusSeconds(60)))
                .toList();
        Upserted second = repository.upsertAll(restaged);

        Assertions.assertEquals(
                new Upserted(2, Set.of(docs.get(0).id(), docs.get(1).id())), first);
        Assertions.assertTrue(decided);
        Assertions.assertEquals(new Upserted(2, Set.of()), second, "a re-stage inserts nothing");
        Assertions.assertEquals(new Upserted(0, Set.of()), repository.upsertAll(List.of()));
        FieldRecommendation kept = repository.find(tenant, docs.get(0).id()).orElseThrow();
        Assertions.assertEquals(ReviewStatus.APPROVED, kept.reviewStatus(), "the decision survives the re-stage");
        Assertions.assertEquals(2L, kept.version());
        Assertions.assertEquals(1, kept.reviewHistory().size());
        Assertions.assertEquals(STAGED, kept.stagedAt());
        Assertions.assertEquals(STAGED.plusSeconds(60), kept.updatedAt(), "only updatedAt is rewritten");
        Assertions.assertEquals(
                docs.get(1),
                withUpdatedAt(repository.find(tenant, docs.get(1).id()).orElseThrow(), STAGED));
        Assertions.assertTrue(repository.find("frrepo-other", docs.get(0).id()).isEmpty(), "reads are tenant-scoped");
    }

    /**
     * Seven open rows, three staged at the same instant so the {@code _id} tie-break runs. A row of
     * the first page is decided before the second page is read; offset paging would then skip a row.
     */
    @Test
    void listPagesByKeysetWithoutSkippingDecidedRows() {
        String tenant = "frrepo-keyset";
        String batch = batchId(tenant, 10);
        List<FieldRecommendation> docs = new ArrayList<>();
        for (int index = 1; index <= 7; index++) {
            Instant stagedAt = index <= 3 ? STAGED : STAGED.plusSeconds(index);
            docs.add(recommendation(
                    tenant, batch, "cert-" + index, "100000000" + index, "loc-" + index, "phone", stagedAt));
        }
        docs.add(recommendation(tenant, batch, "cert-8", "1000000008", "loc-8", "fax", STAGED.plusSeconds(100)));
        repository.upsertAll(docs);
        ListQuery firstQuery = new ListQuery(tenant, "phone", ReviewStatus.open(), null, null, null, 3);

        List<FieldRecommendation> firstPage = page(repository.list(firstQuery), 3);
        boolean decided = transactions.run(session -> repository.decide(
                session,
                tenant,
                firstPage.get(1).id(),
                1L,
                ReviewStatus.open(),
                ReviewStatus.REJECTED,
                history("REJECT", STAGED.plusSeconds(200)),
                STAGED.plusSeconds(200)));
        List<FieldRecommendation> secondPage = page(repository.list(after(firstQuery, firstPage)), 3);
        List<FieldRecommendation> thirdPage = page(repository.list(after(firstQuery, secondPage)), 3);

        Assertions.assertTrue(decided);
        Assertions.assertEquals(4, repository.list(firstQuery).size(), "a full page fetches one more row");
        Assertions.assertEquals(3, firstPage.size());
        Assertions.assertEquals(3, secondPage.size());
        Assertions.assertEquals(1, thirdPage.size());
        Assertions.assertEquals(
                1, repository.list(after(firstQuery, secondPage)).size(), "the last page has no next");
        List<String> seen = new ArrayList<>();
        for (List<FieldRecommendation> page : List.of(firstPage, secondPage, thirdPage)) {
            page.forEach(doc -> seen.add(doc.id()));
        }
        List<String> expected = docs.subList(0, 7).stream()
                .sorted((left, right) -> {
                    int byTime = right.stagedAt().compareTo(left.stagedAt());
                    return byTime != 0 ? byTime : right.id().compareTo(left.id());
                })
                .map(FieldRecommendation::id)
                .toList();
        Assertions.assertEquals(expected, seen, "newest first, no repeat, no gap");
        Assertions.assertEquals(
                List.of(docs.get(6).id()),
                repository
                        .list(new ListQuery(tenant, "phone", ReviewStatus.open(), "1000000007", batch, null, 10))
                        .stream()
                        .map(FieldRecommendation::id)
                        .toList(),
                "npi and batch narrow the list");
        Assertions.assertEquals(
                7,
                repository
                        .list(new ListQuery(tenant, null, ReviewStatus.open(), null, null, null, 10))
                        .size(),
                "without a field: six open phones and the fax");
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> repository.list(new ListQuery(tenant, "phone", ReviewStatus.open(), null, null, "nope", 3)));
    }

    @Test
    void listUsesThePartialIndex() {
        String tenant = "frrepo-explain";
        String batch = batchId(tenant, 10);
        List<FieldRecommendation> docs = new ArrayList<>();
        for (String field : List.of("phone", "fax", "practice_name", "location")) {
            for (int index = 1; index <= 3; index++) {
                docs.add(recommendation(
                        tenant,
                        batch,
                        "cert-" + index,
                        "100000000" + index,
                        "loc-" + index,
                        field,
                        STAGED.plusSeconds(index)));
            }
        }
        repository.upsertAll(docs);
        ListQuery query = new ListQuery(tenant, "phone", ReviewStatus.open(), null, null, null, 3);
        ListQuery next = after(query, page(repository.list(query), 3).subList(0, 1));

        Assertions.assertEquals(Set.of("field_recs_open_list"), indexesOf(query));
        Assertions.assertEquals(Set.of("field_recs_open_list"), indexesOf(next), "the keyset page too");
        Assertions.assertEquals(
                Set.of("field_recs_open_list_any_field"),
                indexesOf(new ListQuery(tenant, null, ReviewStatus.open(), null, null, null, 3)));
    }

    @Test
    void summaryCountsFieldByStatus() {
        String tenant = "frrepo-summary";
        String october = batchId(tenant, 10);
        List<FieldRecommendation> docs = List.of(
                recommendation(tenant, october, "cert-1", "1000000001", "loc-1", "phone", STAGED),
                recommendation(tenant, october, "cert-2", "1000000002", "loc-2", "phone", STAGED),
                recommendation(tenant, october, "cert-3", "1000000003", "loc-3", "phone", STAGED),
                recommendation(tenant, october, "cert-1", "1000000001", null, "first_name", STAGED),
                recommendation(tenant, batchId(tenant, 9), "cert-1", "1000000001", "loc-1", "phone", STAGED));
        repository.upsertAll(docs);
        decideTo(tenant, docs.get(0).id(), ReviewStatus.APPROVED);
        decideTo(tenant, docs.get(1).id(), ReviewStatus.SKIPPED);

        Map<String, Map<ReviewStatus, Long>> summary = repository.summary(tenant, october);

        Assertions.assertEquals(
                Map.of(
                        "phone",
                        Map.of(ReviewStatus.APPROVED, 1L, ReviewStatus.SKIPPED, 1L, ReviewStatus.PENDING, 1L),
                        "first_name",
                        Map.of(ReviewStatus.PENDING, 1L)),
                summary,
                "the September batch is not counted");
        Assertions.assertEquals(Map.of("phone", 2L, "first_name", 1L), repository.countPendingByField(tenant));
        Assertions.assertEquals(Map.of(), repository.summary("frrepo-nobody", october));
    }

    @Test
    void decideIsGuardedByVersionAndStatus() {
        String tenant = "frrepo-decide";
        FieldRecommendation doc =
                recommendation(tenant, batchId(tenant, 10), "cert-1", "1000000001", "loc-1", "phone", STAGED);
        repository.upsertAll(List.of(doc));
        Instant decidedAt = STAGED.plusSeconds(30);

        boolean staleVersion = transactions.run(session -> repository.decide(
                session, tenant, doc.id(), 2L, ReviewStatus.open(), ReviewStatus.APPROVED, null, decidedAt));
        boolean wrongStatus = transactions.run(session -> repository.decide(
                session,
                tenant,
                doc.id(),
                1L,
                Set.of(ReviewStatus.APPROVED),
                ReviewStatus.RELEASE_REQUESTED,
                null,
                decidedAt));
        boolean otherTenant = transactions.run(session -> repository.decide(
                session, "frrepo-other", doc.id(), 1L, ReviewStatus.open(), ReviewStatus.APPROVED, null, decidedAt));
        boolean approved = transactions.run(session -> repository.decide(
                session,
                tenant,
                doc.id(),
                1L,
                ReviewStatus.open(),
                ReviewStatus.APPROVED,
                Updates.combine(
                        history("APPROVE", decidedAt), Updates.set("reviewStatus", ReviewStatus.RELEASED.name())),
                decidedAt));
        boolean replayed = transactions.run(session -> repository.decide(
                session, tenant, doc.id(), 1L, ReviewStatus.open(), ReviewStatus.APPROVED, null, decidedAt));

        Assertions.assertFalse(staleVersion);
        Assertions.assertFalse(wrongStatus);
        Assertions.assertFalse(otherTenant);
        Assertions.assertTrue(approved);
        Assertions.assertFalse(replayed, "the same decision again finds version 2");
        FieldRecommendation stored = repository.find(tenant, doc.id()).orElseThrow();
        Assertions.assertEquals(
                ReviewStatus.APPROVED, stored.reviewStatus(), "a caller's updates cannot set the status");
        Assertions.assertEquals(2L, stored.version());
        Assertions.assertEquals(decidedAt, stored.updatedAt());
        Assertions.assertEquals(
                List.of(new HistoryEntry("APPROVE", "reviewer@example.com", decidedAt, null)), stored.reviewHistory());
    }

    @Test
    void forNpiWithoutBatchTakesTheLatest() {
        String tenant = "frrepo-npi";
        String september = batchId(tenant, 9);
        String october = batchId(tenant, 10);
        repository.upsertAll(List.of(
                recommendation(tenant, september, "cert-1", "1000000001", "loc-1", "phone", STAGED),
                recommendation(tenant, september, "cert-1", "1000000001", null, "last_name", STAGED),
                recommendation(tenant, october, "cert-1", "1000000001", "loc-1", "fax", STAGED),
                recommendation(tenant, october, "cert-2", "1000000002", "loc-2", "fax", STAGED)));

        Assertions.assertEquals(List.of(october), batchesOf(repository.forNpi(tenant, "1000000001", null, 10)));
        Assertions.assertEquals(
                List.of(september, september), batchesOf(repository.forNpi(tenant, "1000000001", september, 10)));
        Assertions.assertEquals(
                1, repository.forNpi(tenant, "1000000001", september, 1).size(), "the limit holds");
        Assertions.assertEquals(List.of(), repository.forNpi(tenant, "1999999999", null, 10));
        Assertions.assertEquals(List.of(), repository.forNpi("frrepo-other", "1000000001", null, 10));
        Assertions.assertEquals(List.of(october), batchesOf(repository.forPractitioner(tenant, "cert-1", null, 10)));
        Assertions.assertEquals(
                List.of(september, september), batchesOf(repository.forPractitioner(tenant, "cert-1", september, 10)));
    }

    /**
     * The release reads Step 7 builds on: claim a release's fields, count another release's
     * {@code RELEASING} fields of the practitioner, and set {@code goldenRecordUpdate} legs by dotted
     * path on a document staged without one.
     */
    @Test
    void releaseMovesAndCounts() {
        String tenant = "frrepo-release";
        String batch = batchId(tenant, 10);
        List<FieldRecommendation> docs = List.of(
                recommendation(tenant, batch, "cert-1", "1000000001", "loc-1", "phone", STAGED),
                recommendation(tenant, batch, "cert-1", "1000000001", "loc-1", "fax", STAGED.plusSeconds(1)),
                recommendation(tenant, batch, "cert-1", "1000000001", "loc-2", "phone", STAGED.plusSeconds(2)),
                recommendation(tenant, batch, "cert-1", "1000000001", null, "last_name", STAGED.minusSeconds(60)));
        Map<String, Long> before = repository.countByStatus();
        repository.upsertAll(docs);
        for (FieldRecommendation doc : docs.subList(0, 3)) {
            decideTo(tenant, doc.id(), ReviewStatus.APPROVED);
        }
        Instant requestedAt = STAGED.plusSeconds(300);
        for (int index = 0; index < 3; index++) {
            String releaseId = index < 2 ? "rel-a" : "rel-b";
            String id = docs.get(index).id();
            Document request = new GoldenRecordUpdate(
                            releaseId,
                            "reviewer@example.com",
                            requestedAt,
                            "SLICE",
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null)
                    .toDocument();
            boolean requested = transactions.run(session -> repository.decide(
                    session,
                    tenant,
                    id,
                    2L,
                    Set.of(ReviewStatus.APPROVED),
                    ReviewStatus.RELEASE_REQUESTED,
                    Updates.set("goldenRecordUpdate", request),
                    requestedAt));
            Assertions.assertTrue(requested, id);
        }

        long claimedA = transactions.run(session -> repository.moveAll(
                session,
                tenant,
                "cert-1",
                "rel-a",
                ReviewStatus.RELEASE_REQUESTED,
                ReviewStatus.RELEASING,
                Updates.set("goldenRecordUpdate.resource", "location"),
                requestedAt.plusSeconds(1)));
        long claimedB = transactions.run(session -> repository.moveAll(
                session,
                tenant,
                "cert-1",
                "rel-b",
                ReviewStatus.RELEASE_REQUESTED,
                ReviewStatus.RELEASING,
                null,
                requestedAt.plusSeconds(2)));

        Assertions.assertEquals(2L, claimedA);
        Assertions.assertEquals(1L, claimedB);
        Assertions.assertEquals(1L, repository.countReleasing(tenant, "cert-1", "rel-a"));
        Assertions.assertEquals(2L, repository.countReleasing(tenant, "cert-1", "rel-b"));
        Assertions.assertEquals(0L, repository.countReleasing(tenant, "cert-2", "rel-a"));
        List<FieldRecommendation> releaseA = repository.forRelease(tenant, "rel-a", 10);
        Assertions.assertEquals(
                List.of(docs.get(1).id(), docs.get(0).id()),
                releaseA.stream().map(FieldRecommendation::id).sorted().toList());
        FieldRecommendation claimed = releaseA.get(0);
        Assertions.assertEquals(ReviewStatus.RELEASING, claimed.reviewStatus());
        Assertions.assertEquals(4L, claimed.version(), "every move bumps the version");
        Assertions.assertEquals("location", claimed.goldenRecordUpdate().resource());
        Assertions.assertEquals("rel-a", claimed.goldenRecordUpdate().releaseId());
        Assertions.assertEquals(requestedAt.plusSeconds(1), claimed.updatedAt());
        Assertions.assertEquals(List.of(), repository.forRelease("frrepo-other", "rel-a", 10));

        Map<String, Long> after = repository.countByStatus();
        Assertions.assertEquals(3L, after.get("RELEASING") - before.getOrDefault("RELEASING", 0L));
        Assertions.assertEquals(1L, after.get("PENDING") - before.getOrDefault("PENDING", 0L));
        Assertions.assertEquals(
                STAGED.minusSeconds(60),
                repository.oldestPendingStagedAt(tenant).orElseThrow());
        Assertions.assertTrue(repository.oldestPendingStagedAt("frrepo-nobody").isEmpty());

        collections
                .fieldRecommendations()
                .updateOne(Filters.eq("_id", docs.get(3).id()), Updates.set("goldenRecordUpdate.releaseId", "rel-c"));
        Assertions.assertEquals(
                "rel-c",
                repository
                        .find(tenant, docs.get(3).id())
                        .orElseThrow()
                        .goldenRecordUpdate()
                        .releaseId(),
                "a staged document has no goldenRecordUpdate, so a dotted $set creates it");
    }

    private static List<FieldRecommendation> page(List<FieldRecommendation> fetched, int limit) {
        return fetched.size() > limit ? fetched.subList(0, limit) : fetched;
    }

    private static ListQuery after(ListQuery query, List<FieldRecommendation> page) {
        String cursor = FieldRecommendationRepository.cursorAfter(page.get(page.size() - 1));
        return new ListQuery(
                query.tenantId(),
                query.field(),
                query.statuses(),
                query.npi(),
                query.exportBatchId(),
                cursor,
                query.limit());
    }

    private void decideTo(String tenant, String id, ReviewStatus to) {
        Instant decidedAt = STAGED.plusSeconds(10);
        boolean decided = transactions.run(session -> repository.decide(
                session, tenant, id, 1L, ReviewStatus.open(), to, history(to.name(), decidedAt), decidedAt));
        Assertions.assertTrue(decided, id);
    }

    private static Bson history(String action, Instant at) {
        return Updates.push("reviewHistory", new HistoryEntry(action, "reviewer@example.com", at, null).toDocument());
    }

    /** Every index the winning plan of {@code query}'s find names. */
    private Set<String> indexesOf(ListQuery query) {
        Document explain = collections
                .fieldRecommendations()
                .find(FieldRecommendationRepository.listFilter(query))
                .sort(FieldRecommendationRepository.LIST_SORT)
                .limit(query.limit() + 1)
                .explain();
        String plan = explain.get("queryPlanner", Document.class)
                .get("winningPlan", Document.class)
                .toJson();
        Set<String> names = new HashSet<>();
        INDEX_NAME.matcher(plan).results().forEach(match -> names.add(match.group(1)));
        Assertions.assertFalse(names.isEmpty(), plan);
        return names;
    }

    private static List<String> batchesOf(List<FieldRecommendation> docs) {
        return docs.stream().map(FieldRecommendation::exportBatchId).toList();
    }

    private static String batchId(String tenant, int month) {
        return Ids.batchId(tenant, "candor", java.time.YearMonth.of(2026, month), 1);
    }

    private static FieldRecommendation withUpdatedAt(FieldRecommendation doc, Instant updatedAt) {
        return new FieldRecommendation(
                doc.id(),
                doc.tenantId(),
                doc.sourceType(),
                doc.source(),
                doc.exportBatchId(),
                doc.practitioner(),
                doc.location(),
                doc.field(),
                doc.sentValue(),
                doc.vendorValue(),
                doc.vendorStatus(),
                doc.vendorReason(),
                doc.vendorConfidence(),
                doc.vendorEvidence(),
                doc.reviewStatus(),
                doc.reviewHistory(),
                doc.version(),
                doc.goldenRecordUpdate(),
                doc.sourceFile(),
                doc.stagedAt(),
                updatedAt);
    }

    /** A {@code PENDING} recommendation as staging would write it; a null location is practitioner-level. */
    private static FieldRecommendation recommendation(
            String tenant,
            String exportBatchId,
            String practitionerId,
            String npi,
            String locationId,
            String field,
            Instant stagedAt) {
        return new FieldRecommendation(
                Ids.fieldRecommendationId(tenant, "candor", exportBatchId, practitionerId, locationId, field),
                tenant,
                "vendor",
                "candor",
                exportBatchId,
                new Practitioner(practitionerId, npi),
                locationId == null ? null : new Location(locationId),
                field,
                "sent",
                "vendor",
                "INVALID",
                List.of("direct_outreach"),
                "HIGH",
                List.of(Map.of("reason", "direct_outreach", "evidence", "date", "value", "2026-06-17")),
                ReviewStatus.PENDING,
                List.of(),
                1L,
                null,
                new SourceFile("vendor-sftp|to/" + tenant + "/f.csv|1", SHEET, 913),
                stagedAt,
                stagedAt);
    }
}
