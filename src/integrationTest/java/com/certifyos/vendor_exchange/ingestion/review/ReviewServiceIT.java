package com.certifyos.vendor_exchange.ingestion.review;

import com.certifyos.vendor_exchange.ApiTestProfile;
import com.certifyos.vendor_exchange.MongoResource;
import com.certifyos.vendor_exchange.audit.AuditEvent;
import com.certifyos.vendor_exchange.audit.AuditEventType;
import com.certifyos.vendor_exchange.audit.AuditRepository;
import com.certifyos.vendor_exchange.http.ProblemException;
import com.certifyos.vendor_exchange.ingestion.ReviewFixtures;
import com.certifyos.vendor_exchange.ingestion.review.ReviewService.DecisionRequest;
import com.certifyos.vendor_exchange.ingestion.review.ReviewService.DecisionResult;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendationRepository;
import com.certifyos.vendor_exchange.ingestion.staging.ReviewStatus;
import com.certifyos.vendor_exchange.ingestion.staging.ReviewStatus.Action;
import com.certifyos.vendor_exchange.metrics.VendorExchangeMetrics;
import com.certifyos.vendor_exchange.persistence.Collections;
import com.certifyos.vendor_exchange.persistence.Ids;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.model.Filters;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectSpy;
import jakarta.inject.Inject;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/**
 * Reviewer decisions through {@link ReviewService}: one guarded update and one {@code FIELD_*} event
 * in one transaction, the 400, 404 and 409 answers, and the bulk form. Every tenant here starts with
 * {@code org-revsvc}; each test decides its own documents.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
class ReviewServiceIT {

    static final String TENANT = "org-revsvc";
    static final String REVIEWER = "reviewer-7@certifyos.com";
    static final String SECOND = "reviewer-3@certifyos.com";

    @Inject
    ReviewService service;

    @Inject
    FieldRecommendationRepository repository;

    @Inject
    DecisionWriter writer;

    @Inject
    AuditRepository audit;

    @Inject
    Collections collections;

    @Inject
    ObjectMapper mapper;

    @Inject
    MeterRegistry registry;

    @Inject
    ManagedExecutor executor;

    @InjectSpy
    RejectedValueRepository rejectedValues;

    @Test
    void approveFromPendingWritesHistoryAndEvent() {
        FieldRecommendation doc = stage(1, "cert-a1", "loc-7781", "practice_name");
        double before = decisions("APPROVE");

        FieldRecommendation approved =
                service.decide(TENANT, new DecisionRequest(doc.id(), Action.APPROVE, null, 1L), REVIEWER);

        Assertions.assertEquals(ReviewStatus.APPROVED, approved.reviewStatus());
        Assertions.assertEquals(2L, approved.version());
        Assertions.assertEquals(
                approved, repository.find(TENANT, doc.id()).orElseThrow(), "the answer is the stored document");
        Assertions.assertEquals(1, approved.reviewHistory().size());
        FieldRecommendation.HistoryEntry entry = approved.reviewHistory().get(0);
        Assertions.assertEquals("APPROVE", entry.action());
        Assertions.assertEquals(REVIEWER, entry.by());
        Assertions.assertNull(entry.comment());
        Assertions.assertEquals(approved.updatedAt(), entry.at());
        List<AuditEvent> trail = audit.trailForField(TENANT, doc.id(), 10);
        Assertions.assertEquals(1, trail.size());
        AuditEvent event = trail.get(0);
        Assertions.assertEquals(AuditEventType.FIELD_APPROVED, event.type());
        Assertions.assertEquals(doc.id(), event.fieldRecommendationId());
        Assertions.assertEquals("cert-a1", event.certifyPractitionerId());
        Assertions.assertEquals(doc.exportBatchId(), event.exportBatchId());
        Assertions.assertEquals(doc.sourceFile().uploadId(), event.uploadId());
        Assertions.assertEquals("candor", event.vendor());
        Assertions.assertEquals(REVIEWER, event.actor());
        Assertions.assertNull(event.attempt());
        Assertions.assertEquals(
                Map.of(
                        "field",
                        "practice_name",
                        "certifyLocationId",
                        "loc-7781",
                        "fromStatus",
                        "PENDING",
                        "version",
                        2L),
                event.detail());
        Document stored =
                collections.reviewEvents().find(Filters.eq("id", event.id())).first();
        Assertions.assertNotNull(stored, "field events go to vendor_review_events");
        Assertions.assertNull(
                collections.events().find(Filters.eq("id", event.id())).first());
        Assertions.assertEquals(before + 1, decisions("APPROVE"));
    }

    @Test
    void rejectNeedsComment() {
        FieldRecommendation doc = stage(2, "cert-r1", "loc-1", "phone");

        assertProblem(
                400,
                "COMMENT_REQUIRED",
                () -> service.decide(TENANT, new DecisionRequest(doc.id(), Action.REJECT, null, 1L), REVIEWER));
        assertProblem(
                400,
                "COMMENT_REQUIRED",
                () -> service.decide(TENANT, new DecisionRequest(doc.id(), Action.REJECT, "  ", 1L), REVIEWER));
        assertProblem(
                400,
                "VERSION_REQUIRED",
                () -> service.decide(TENANT, new DecisionRequest(doc.id(), Action.APPROVE, null, null), REVIEWER));
        assertProblem(
                400,
                "INVALID_REQUEST",
                () -> service.decide(TENANT, new DecisionRequest(doc.id(), null, null, 1L), REVIEWER));
        assertProblem(
                400,
                "INVALID_REQUEST",
                () -> service.decide(TENANT, new DecisionRequest(doc.id(), Action.ROLLBACK, "x", 1L), REVIEWER));
        assertProblem(
                400,
                "INVALID_REQUEST",
                () -> service.decide(TENANT, new DecisionRequest(doc.id(), Action.RELEASE, "x", 1L), REVIEWER));
        Assertions.assertEquals(
                doc, repository.find(TENANT, doc.id()).orElseThrow(), "a refused request writes nothing");
        Assertions.assertEquals(List.of(), audit.trailForField(TENANT, doc.id(), 10));

        FieldRecommendation rejected = service.decide(
                TENANT, new DecisionRequest(doc.id(), Action.REJECT, "Office confirmed old number", 1L), REVIEWER);

        Assertions.assertEquals(ReviewStatus.REJECTED, rejected.reviewStatus());
        Assertions.assertEquals(
                "Office confirmed old number", rejected.reviewHistory().get(0).comment());
        AuditEvent event = audit.trailForField(TENANT, doc.id(), 10).get(0);
        Assertions.assertEquals(AuditEventType.FIELD_REJECTED, event.type());
        Assertions.assertEquals("Office confirmed old number", event.detail().get("comment"));
        Assertions.assertEquals("PENDING", event.detail().get("fromStatus"));
    }

    @Test
    void skipThenApproveIsAllowedAndFinal() {
        FieldRecommendation doc = stage(3, "cert-s1", null, "last_name");

        FieldRecommendation skipped =
                service.decide(TENANT, new DecisionRequest(doc.id(), Action.SKIP, null, 1L), REVIEWER);
        FieldRecommendation approved =
                service.decide(TENANT, new DecisionRequest(doc.id(), Action.APPROVE, null, 2L), SECOND);

        Assertions.assertEquals(ReviewStatus.SKIPPED, skipped.reviewStatus());
        Assertions.assertEquals(ReviewStatus.APPROVED, approved.reviewStatus());
        Assertions.assertEquals(3L, approved.version());
        Assertions.assertEquals(
                List.of("SKIP", "APPROVE"),
                approved.reviewHistory().stream()
                        .map(FieldRecommendation.HistoryEntry::action)
                        .toList());
        assertProblem(
                409,
                "DECISION_CONFLICT",
                () -> service.decide(TENANT, new DecisionRequest(doc.id(), Action.REJECT, "late", 3L), REVIEWER));
        List<AuditEvent> trail = audit.trailForField(TENANT, doc.id(), 10);
        Assertions.assertEquals(
                List.of(AuditEventType.FIELD_SKIPPED, AuditEventType.FIELD_APPROVED),
                trail.stream().map(AuditEvent::type).toList());
        Assertions.assertEquals("SKIPPED", trail.get(1).detail().get("fromStatus"));
        Assertions.assertFalse(
                trail.get(0).detail().containsKey("certifyLocationId"), "a practitioner-level field names no location");
    }

    @Test
    void skipAfterApproveIs409() {
        FieldRecommendation doc = stage(4, "cert-k1", "loc-1", "fax");
        service.decide(TENANT, new DecisionRequest(doc.id(), Action.APPROVE, null, 1L), REVIEWER);
        double conflicts = conflicts();

        ProblemException conflict = assertProblem(
                409,
                "DECISION_CONFLICT",
                () -> service.decide(TENANT, new DecisionRequest(doc.id(), Action.SKIP, null, 2L), REVIEWER));

        Map<String, Object> detail = detail(conflict);
        Assertions.assertEquals("APPROVED", detail.get("reviewStatus"));
        Assertions.assertEquals(2, ((Number) detail.get("version")).intValue());
        Assertions.assertEquals(conflicts + 1, conflicts());
        Assertions.assertEquals(1, audit.trailForField(TENANT, doc.id(), 10).size());
    }

    @Test
    void retriedDecisionIs409WithOwnDecisionVisible() {
        FieldRecommendation doc = stage(5, "cert-t1", "loc-1", "phone");
        DecisionRequest approve = new DecisionRequest(doc.id(), Action.APPROVE, null, 1L);

        service.decide(TENANT, approve, REVIEWER);
        ProblemException retried =
                assertProblem(409, "DECISION_CONFLICT", () -> service.decide(TENANT, approve, REVIEWER));

        Map<String, Object> detail = detail(retried);
        Assertions.assertEquals(doc.id(), detail.get("id"));
        Assertions.assertEquals("APPROVED", detail.get("reviewStatus"));
        Assertions.assertEquals(2, ((Number) detail.get("version")).intValue());
        Assertions.assertEquals(REVIEWER, detail.get("lastDecisionBy"), "the UI sees its own decision");
        Assertions.assertEquals(1, audit.trailForField(TENANT, doc.id(), 10).size(), "one event, not two");
        Assertions.assertEquals(
                2L, repository.find(TENANT, doc.id()).orElseThrow().version());
    }

    /** Two reviewers send a decision on the same version at the same moment; the database lets one through. */
    @Test
    void twoReviewersOneWins() throws Exception {
        FieldRecommendation doc = stage(6, "cert-w1", "loc-1", "phone");
        List<String> reviewers = List.of(REVIEWER, SECOND);
        CyclicBarrier start = new CyclicBarrier(reviewers.size());
        List<CompletableFuture<Integer>> answers = new ArrayList<>();
        for (String reviewer : reviewers) {
            answers.add(CompletableFuture.supplyAsync(
                    () -> {
                        await(start);
                        try {
                            service.decide(TENANT, new DecisionRequest(doc.id(), Action.APPROVE, null, 1L), reviewer);
                            return 200;
                        } catch (ProblemException refused) {
                            return refused.status();
                        }
                    },
                    executor));
        }
        CompletableFuture.allOf(answers.toArray(CompletableFuture[]::new)).get(60, TimeUnit.SECONDS);

        List<Integer> statuses = new ArrayList<>();
        for (CompletableFuture<Integer> answer : answers) {
            statuses.add(answer.get());
        }
        java.util.Collections.sort(statuses);
        Assertions.assertEquals(List.of(200, 409), statuses, "exactly one reviewer wins");
        Assertions.assertEquals(1, audit.trailForField(TENANT, doc.id(), 10).size());
        FieldRecommendation stored = repository.find(TENANT, doc.id()).orElseThrow();
        Assertions.assertEquals(2L, stored.version());
        Assertions.assertEquals(1, stored.reviewHistory().size());
    }

    /**
     * The race the pre-check cannot see: the document moves after the service read it. The guarded
     * update then matches nothing and the transaction writes no event.
     */
    @Test
    void aWriteFromAStaleReadIsRefusedByTheGuard() {
        FieldRecommendation doc = stage(9, "cert-g1", "loc-1", "phone");
        service.decide(TENANT, new DecisionRequest(doc.id(), Action.SKIP, null, 1L), SECOND);

        boolean recorded = writer.record(doc, Action.APPROVE, null, REVIEWER, ReviewFixtures.STAGED.plusSeconds(60));

        Assertions.assertFalse(recorded);
        FieldRecommendation stored = repository.find(TENANT, doc.id()).orElseThrow();
        Assertions.assertEquals(ReviewStatus.SKIPPED, stored.reviewStatus());
        Assertions.assertEquals(2L, stored.version());
        Assertions.assertEquals(
                List.of(AuditEventType.FIELD_SKIPPED),
                audit.trailForField(TENANT, doc.id(), 10).stream()
                        .map(AuditEvent::type)
                        .toList());
    }

    @Test
    void bulkIsPerItem() {
        FieldRecommendation first = stage(7, "cert-b1", "loc-1", "phone");
        FieldRecommendation second = stage(7, "cert-b2", "loc-2", "phone");
        service.decide(TENANT, new DecisionRequest(second.id(), Action.APPROVE, null, 1L), SECOND);
        String missing = Ids.fieldRecommendationId(TENANT, "candor", batch(7), "cert-b9", null, "phone");

        List<DecisionResult> results = service.decideAll(
                TENANT,
                List.of(
                        new DecisionRequest(first.id(), Action.APPROVE, null, 1L),
                        new DecisionRequest(second.id(), Action.REJECT, "no", 1L),
                        new DecisionRequest(missing, Action.APPROVE, null, 1L),
                        new DecisionRequest(first.id(), Action.REJECT, null, 2L)),
                REVIEWER);

        Assertions.assertEquals(
                List.of(first.id(), second.id(), missing, first.id()),
                results.stream().map(DecisionResult::id).toList());
        Assertions.assertEquals(
                List.of(200, 409, 404, 400),
                results.stream().map(DecisionResult::status).toList());
        Assertions.assertEquals(
                Arrays.asList(null, "DECISION_CONFLICT", "FIELD_NOT_FOUND", "COMMENT_REQUIRED"),
                results.stream().map(DecisionResult::code).toList());
        Assertions.assertEquals(ReviewStatus.APPROVED, results.get(0).document().reviewStatus());
        Assertions.assertEquals(
                SECOND, results.get(1).document().reviewHistory().get(0).by(), "the 409 carries the current document");
        Assertions.assertNull(results.get(2).document());
        Assertions.assertNull(results.get(3).document());

        List<DecisionRequest> tooMany =
                java.util.Collections.nCopies(201, new DecisionRequest(first.id(), Action.SKIP, null, 2L));
        assertProblem(400, "INVALID_REQUEST", () -> service.decideAll(TENANT, tooMany, REVIEWER));
        assertProblem(400, "INVALID_REQUEST", () -> service.decideAll(TENANT, List.of(), REVIEWER));
        Assertions.assertEquals(
                1, audit.trailForField(TENANT, first.id(), 10).size(), "nothing of the refused bulk was written");
    }

    @Test
    void anotherTenantsDocumentIs404() {
        String theirs = "org-revsvc-other";
        FieldRecommendation doc = ReviewFixtures.pending(
                theirs,
                Ids.batchId(theirs, "candor", YearMonth.of(2026, 10), 8),
                "cert-o1",
                "1000000801",
                "loc-1",
                "phone",
                "x",
                ReviewFixtures.STAGED);
        repository.upsertAll(List.of(doc));

        assertProblem(
                404,
                "FIELD_NOT_FOUND",
                () -> service.decide(TENANT, new DecisionRequest(doc.id(), Action.APPROVE, null, 1L), REVIEWER));
        Assertions.assertEquals(
                ReviewStatus.PENDING,
                repository.find(theirs, doc.id()).orElseThrow().reviewStatus());
        Assertions.assertEquals(List.of(), audit.trailForField(theirs, doc.id(), 10));
    }

    @Test
    void rejectWritesTheRejectedValue() throws Exception {
        FieldRecommendation doc = stage(11, "cert-000123", "loc-7781", "phone");

        FieldRecommendation rejected = service.decide(
                TENANT,
                new DecisionRequest(doc.id(), Action.REJECT, "Office confirmed old number still active", 1L),
                REVIEWER);

        String sha = java.util.HexFormat.of()
                .formatHex(java.security.MessageDigest.getInstance("SHA-256")
                        .digest("3036790177".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        String expectedId = TENANT + "|cert-000123|loc-7781|phone|" + sha;
        RejectedValue row = rejectedValues.find(TENANT, expectedId).orElseThrow();
        Assertions.assertEquals("3036790177", row.normalizedValue());
        Assertions.assertEquals("(303) 679-0177", row.rawValue());
        Assertions.assertEquals(1, row.normalizationVersion());
        Assertions.assertEquals(1L, row.rejectionCount());
        Assertions.assertEquals(rejected.updatedAt(), row.lastRejectedAt(), "the decision time");
        Assertions.assertEquals(
                List.of(new RejectedValue.Rejection(
                        doc.exportBatchId(),
                        REVIEWER,
                        rejected.updatedAt(),
                        "Office confirmed old number still active")),
                row.rejections());
        Assertions.assertEquals(
                rejected.updatedAt()
                        .atZone(java.time.ZoneOffset.UTC)
                        .plusMonths(12)
                        .toInstant(),
                row.expiresAt());
        AuditEvent event = audit.trailForField(TENANT, doc.id(), 10).get(0);
        Assertions.assertEquals(AuditEventType.FIELD_REJECTED, event.type());
        Assertions.assertEquals(expectedId, event.detail().get("rejectedValueId"));
        Assertions.assertEquals(1L, ((Number) event.detail().get("rejectionCount")).longValue());

        FieldRecommendation approved = stage(12, "cert-000123", "loc-7781", "fax");
        service.decide(TENANT, new DecisionRequest(approved.id(), Action.APPROVE, null, 1L), REVIEWER);
        Assertions.assertFalse(
                audit.trailForField(TENANT, approved.id(), 10).get(0).detail().containsKey("rejectedValueId"),
                "only a rejection touches the memory");
        Assertions.assertEquals(
                1, rejectedValues.forPractitioner(TENANT, "cert-000123", 10).size());
    }

    @Test
    void rejectRollsBackWhenTheMemoryWriteFails() {
        FieldRecommendation doc = stage(13, "cert-rb1", "loc-1", "phone");
        Mockito.doThrow(new IllegalStateException("memory down"))
                .when(rejectedValues)
                .upsert(
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.anyInt());

        Assertions.assertThrows(
                IllegalStateException.class,
                () -> service.decide(TENANT, new DecisionRequest(doc.id(), Action.REJECT, "wrong", 1L), REVIEWER));

        FieldRecommendation stored = repository.find(TENANT, doc.id()).orElseThrow();
        Assertions.assertEquals(
                ReviewStatus.PENDING, stored.reviewStatus(), "the decision rolled back with the memory");
        Assertions.assertEquals(1L, stored.version());
        Assertions.assertEquals(List.of(), stored.reviewHistory());
        Assertions.assertEquals(List.of(), audit.trailForField(TENANT, doc.id(), 10), "no event");
        Assertions.assertEquals(List.of(), rejectedValues.forPractitioner(TENANT, "cert-rb1", 10));
    }

    // helpers

    static String batch(int seq) {
        return Ids.batchId(TENANT, "candor", YearMonth.of(2026, 10), seq);
    }

    FieldRecommendation stage(int seq, String practitionerId, String locationId, String field) {
        FieldRecommendation doc = ReviewFixtures.pending(
                TENANT,
                batch(seq),
                practitionerId,
                "1003002890",
                locationId,
                field,
                "(303) 679-0177",
                ReviewFixtures.STAGED);
        Assertions.assertEquals(
                1, repository.upsertAll(List.of(doc)).insertedIds().size(), "fixture ids are unique");
        return doc;
    }

    static ProblemException assertProblem(int status, String code, Executable call) {
        ProblemException refused = Assertions.assertThrows(ProblemException.class, call);
        Assertions.assertEquals(status, refused.status(), refused.getMessage());
        Assertions.assertEquals(code, refused.code(), refused.getMessage());
        return refused;
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> detail(ProblemException conflict) {
        try {
            return mapper.readValue(conflict.getMessage(), Map.class);
        } catch (java.io.IOException notJson) {
            throw new AssertionError("409 detail is not JSON: " + conflict.getMessage(), notJson);
        }
    }

    double decisions(String action) {
        Counter found = registry.find(VendorExchangeMetrics.INGESTION_REVIEW_DECISIONS)
                .tag("tenant", TENANT)
                .tag("action", action)
                .counter();
        return found == null ? 0 : found.count();
    }

    double conflicts() {
        Counter found = registry.find(VendorExchangeMetrics.INGESTION_REVIEW_CONFLICTS_409)
                .tag("tenant", TENANT)
                .counter();
        return found == null ? 0 : found.count();
    }

    static void await(CyclicBarrier start) {
        try {
            start.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        } catch (java.util.concurrent.BrokenBarrierException | java.util.concurrent.TimeoutException notTogether) {
            throw new IllegalStateException("the reviewers never ran at the same time", notTogether);
        }
    }
}
