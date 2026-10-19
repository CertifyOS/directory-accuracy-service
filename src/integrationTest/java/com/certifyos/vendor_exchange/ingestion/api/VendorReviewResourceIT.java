package com.certifyos.vendor_exchange.ingestion.api;

import com.certifyos.vendor_exchange.ApiTestProfile;
import com.certifyos.vendor_exchange.MongoResource;
import com.certifyos.vendor_exchange.audit.AuditEvent;
import com.certifyos.vendor_exchange.audit.AuditEventType;
import com.certifyos.vendor_exchange.audit.AuditRepository;
import com.certifyos.vendor_exchange.auth.GoogleIdTokenService;
import com.certifyos.vendor_exchange.auth.UserContextFilter;
import com.certifyos.vendor_exchange.auth.WireMockDal;
import com.certifyos.vendor_exchange.export.batch.BatchState;
import com.certifyos.vendor_exchange.export.batch.ExportBatch;
import com.certifyos.vendor_exchange.export.batch.ExportBatchRepository;
import com.certifyos.vendor_exchange.export.batch.ExportNpiRepository;
import com.certifyos.vendor_exchange.export.jobs.EgressFixtures;
import com.certifyos.vendor_exchange.http.Problem;
import com.certifyos.vendor_exchange.ingestion.InboundFixtures;
import com.certifyos.vendor_exchange.ingestion.ReviewFixtures;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendationRepository;
import com.certifyos.vendor_exchange.ingestion.staging.ReviewStatus;
import com.certifyos.vendor_exchange.persistence.Collections;
import com.certifyos.vendor_exchange.persistence.Ids;
import com.certifyos.vendor_exchange.persistence.Transactions;
import com.mongodb.client.model.Updates;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.security.TestSecurity;
import io.quarkus.test.security.jwt.Claim;
import io.quarkus.test.security.jwt.JwtSecurity;
import io.restassured.RestAssured;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import jakarta.inject.Inject;
import java.time.Instant;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The review reads through the endpoints: the keyset list, one document, its trail, the summary and
 * the practitioner and NPI views. The tenant comes from the caller's context; another tenant's
 * document is 404. Unique email and tenant for this class (finding 72); each test stages into its
 * own batch id, so the tenant-wide list is read per batch.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockDal.class)
@TestProfile(ApiTestProfile.class)
@TestSecurity(user = "reviewer")
@JwtSecurity(claims = {@Claim(key = UserContextFilter.EMAIL_CLAIM, value = VendorReviewResourceIT.REVIEWER)})
class VendorReviewResourceIT {

    static final String REVIEWER = "reviewer@certifyos.com";
    static final String TENANT = "org-review";
    static final String OTHER = "org-review-other";
    static final String BASE = "/v1/vendor-review";
    static final Instant STAGED = ReviewFixtures.STAGED;

    @Inject
    FieldRecommendationRepository repository;

    @Inject
    AuditRepository audit;

    @Inject
    Transactions transactions;

    @Inject
    ExportBatchRepository batches;

    @Inject
    Collections collections;

    @InjectMock
    GoogleIdTokenService google;

    @BeforeEach
    void before() {
        Mockito.when(google.idToken("test-dal-iap-client-id")).thenReturn("dal-id-token");
        WireMockDal.stubMember(REVIEWER, TENANT);
    }

    static RequestSpecification member() {
        return RestAssured.given().header("tenant-id", TENANT).contentType("application/json");
    }

    @Test
    void listDefaultsToPendingNewestFirst() {
        String batch = batch(1);
        FieldRecommendation oldest = stage(doc(batch, "cert-l1", "1000000101", "loc-1", "phone", STAGED));
        FieldRecommendation middle =
                stage(doc(batch, "cert-l2", "1000000102", "loc-2", "phone", STAGED.plusSeconds(10)));
        FieldRecommendation newest =
                stage(doc(batch, "cert-l3", "1000000103", null, "last_name", STAGED.plusSeconds(20)));
        FieldRecommendation skipped =
                stage(doc(batch, "cert-l4", "1000000104", "loc-4", "fax", STAGED.plusSeconds(30)));
        move(skipped, ReviewStatus.SKIPPED);

        member().queryParam("exportBatchId", batch)
                .get(BASE + "/fields")
                .then()
                .statusCode(200)
                .body("items.id", Matchers.contains(newest.id(), middle.id(), oldest.id()))
                .body("items.reviewStatus", Matchers.everyItem(Matchers.equalTo("PENDING")))
                .body("next", Matchers.nullValue());
        member().queryParam("exportBatchId", batch)
                .queryParam("reviewStatus", "PENDING", "SKIPPED")
                .get(BASE + "/fields")
                .then()
                .statusCode(200)
                .body("items.id", Matchers.contains(skipped.id(), newest.id(), middle.id(), oldest.id()));
        member().queryParam("exportBatchId", batch)
                .queryParam("reviewStatus", "SKIPPED")
                .get(BASE + "/fields")
                .then()
                .statusCode(200)
                .body("items.id", Matchers.contains(skipped.id()))
                .body("items[0].version", Matchers.equalTo(2));
    }

    @Test
    void listPagesWithNext() {
        String batch = batch(2);
        Set<String> staged = new HashSet<>();
        for (int index = 0; index < 15; index++) {
            staged.add(stage(doc(
                            batch,
                            "cert-p" + index,
                            "10000002" + String.format("%02d", index),
                            "loc-" + index,
                            "phone",
                            STAGED.plusSeconds(index % 4)))
                    .id());
        }

        ExtractableResponse<Response> first = member().queryParam("exportBatchId", batch)
                .queryParam("limit", 10)
                .get(BASE + "/fields")
                .then()
                .statusCode(200)
                .body("items", Matchers.hasSize(10))
                .body("next", Matchers.notNullValue())
                .extract();
        String next = first.path("next");
        Assertions.assertFalse(next.contains("|"), "the cursor is opaque");
        ExtractableResponse<Response> second = member().queryParam("exportBatchId", batch)
                .queryParam("limit", 10)
                .queryParam("after", next)
                .get(BASE + "/fields")
                .then()
                .statusCode(200)
                .body("items", Matchers.hasSize(5))
                .body("next", Matchers.nullValue())
                .extract();
        List<String> seen = new ArrayList<>(first.path("items.id"));
        seen.addAll(second.path("items.id"));
        Assertions.assertEquals(15, seen.size());
        Assertions.assertEquals(staged, new HashSet<>(seen), "no repeat, no gap");

        member().queryParam("limit", 5)
                .get(BASE + "/fields")
                .then()
                .statusCode(400)
                .body("code", Matchers.equalTo("INVALID_REQUEST"));
        member().queryParam("limit", 201)
                .get(BASE + "/fields")
                .then()
                .statusCode(400)
                .body("code", Matchers.equalTo("INVALID_REQUEST"));
        member().queryParam("after", "not-a-cursor")
                .get(BASE + "/fields")
                .then()
                .statusCode(400)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("INVALID_REQUEST"));
    }

    @Test
    void listFiltersByFieldAndNpi() {
        String batch = batch(3);
        FieldRecommendation phoneA = stage(doc(batch, "cert-f1", "1000000301", "loc-1", "phone", STAGED));
        FieldRecommendation faxA = stage(doc(batch, "cert-f1", "1000000301", "loc-1", "fax", STAGED));
        FieldRecommendation phoneB = stage(doc(batch, "cert-f2", "1000000302", "loc-2", "phone", STAGED));

        member().queryParam("exportBatchId", batch)
                .queryParam("field", "phone")
                .get(BASE + "/fields")
                .then()
                .statusCode(200)
                .body("items.id", Matchers.containsInAnyOrder(phoneA.id(), phoneB.id()));
        member().queryParam("exportBatchId", batch)
                .queryParam("field", "phone")
                .queryParam("npi", "1000000301")
                .get(BASE + "/fields")
                .then()
                .statusCode(200)
                .body("items.id", Matchers.contains(phoneA.id()));
        member().queryParam("exportBatchId", batch)
                .queryParam("npi", "1000000301")
                .get(BASE + "/fields")
                .then()
                .statusCode(200)
                .body("items.id", Matchers.containsInAnyOrder(phoneA.id(), faxA.id()));
    }

    @Test
    void anotherTenantsDocumentIs404() {
        String batch = batch(4);
        FieldRecommendation ours = stage(doc(batch, "cert-t1", "1000000401", "loc-7781", "practice_name", STAGED));
        String theirBatch = Ids.batchId(OTHER, "candor", YearMonth.of(2026, 10), 4);
        FieldRecommendation theirs = stage(ReviewFixtures.pending(
                OTHER, theirBatch, "cert-t1", "1000000401", "loc-7781", "practice_name", "x", STAGED));

        member().get(BASE + "/fields/{id}", ours.id())
                .then()
                .statusCode(200)
                .body("id", Matchers.equalTo(ours.id()))
                .body("tenantId", Matchers.equalTo(TENANT))
                .body("sourceType", Matchers.equalTo("vendor"))
                .body("source", Matchers.equalTo("candor"))
                .body("exportBatchId", Matchers.equalTo(batch))
                .body("practitioner.certifyPractitionerId", Matchers.equalTo("cert-t1"))
                .body("practitioner.npi", Matchers.equalTo("1000000401"))
                .body("location.certifyLocationId", Matchers.equalTo("loc-7781"))
                .body("field", Matchers.equalTo("practice_name"))
                .body("vendorValue", Matchers.equalTo("Greenwood Village"))
                .body("vendorStatus", Matchers.equalTo("INVALID"))
                .body("vendorReason", Matchers.contains("direct_outreach"))
                .body("vendorConfidence", Matchers.equalTo("VERY HIGH"))
                .body("vendorEvidence[0].reason", Matchers.equalTo("direct_outreach"))
                .body("vendorEvidence[0].value", Matchers.equalTo("2026-06-17"))
                .body("reviewStatus", Matchers.equalTo("PENDING"))
                .body("reviewHistory", Matchers.empty())
                .body("version", Matchers.equalTo(1))
                .body("goldenRecordUpdate", Matchers.nullValue())
                .body("sourceFile.sheet", Matchers.equalTo(ReviewFixtures.SHEET))
                .body("sourceFile.row", Matchers.equalTo(913))
                .body("stagedAt", Matchers.equalTo("2026-10-15T09:15:10Z"));
        member().get(BASE + "/fields/{id}", theirs.id())
                .then()
                .statusCode(404)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("FIELD_NOT_FOUND"));
        member().get(BASE + "/fields/{id}/events", theirs.id())
                .then()
                .statusCode(404)
                .body("code", Matchers.equalTo("FIELD_NOT_FOUND"));
        member().get(BASE + "/fields/{id}", "org-review|candor|nope|cert-x|-|phone")
                .then()
                .statusCode(404)
                .body("code", Matchers.equalTo("FIELD_NOT_FOUND"));
        member().queryParam("exportBatchId", theirBatch)
                .get(BASE + "/fields")
                .then()
                .statusCode(200)
                .body("items", Matchers.empty());
        member().get(BASE + "/practitioners/{id}", "cert-t1")
                .then()
                .statusCode(200)
                .body("fields.id", Matchers.contains(ours.id()));
        member().body("{\"action\":\"APPROVE\",\"version\":1}")
                .post(BASE + "/fields/{id}/decision", theirs.id())
                .then()
                .statusCode(404)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("FIELD_NOT_FOUND"));
        member().body("{\"decisions\":[{\"id\":\"" + theirs.id() + "\",\"action\":\"REJECT\","
                        + "\"comment\":\"not ours\",\"version\":1}]}")
                .post(BASE + "/decisions")
                .then()
                .statusCode(207)
                .body("results.status", Matchers.contains(404))
                .body("results[0].code", Matchers.equalTo("FIELD_NOT_FOUND"));
        FieldRecommendation untouched = repository.find(OTHER, theirs.id()).orElseThrow();
        Assertions.assertEquals(
                ReviewStatus.PENDING, untouched.reviewStatus(), "another tenant's write changed nothing");
        Assertions.assertEquals(1L, untouched.version());
    }

    @Test
    void summaryCounts() {
        ExportBatch delivered = deliveredBatch(TENANT, "summary");
        ExportBatch theirs = deliveredBatch(OTHER, "summary");
        String batch = delivered.id();
        stage(doc(batch, "cert-s1", "1000000501", "loc-1", "phone", STAGED));
        stage(doc(batch, "cert-s2", "1000000502", "loc-2", "phone", STAGED));
        move(stage(doc(batch, "cert-s3", "1000000503", "loc-3", "phone", STAGED)), ReviewStatus.APPROVED);
        stage(doc(batch, "cert-s1", "1000000501", "loc-1", "fax", STAGED));

        member().queryParam("exportBatchId", batch)
                .get(BASE + "/fields/summary")
                .then()
                .statusCode(200)
                .body("fields.phone.PENDING", Matchers.equalTo(2))
                .body("fields.phone.APPROVED", Matchers.equalTo(1))
                .body("fields.fax.PENDING", Matchers.equalTo(1))
                .body("fields.fax", Matchers.not(Matchers.hasKey("APPROVED")))
                .body("exportBatchState", Matchers.equalTo("DELIVERED"));
        member().queryParam("exportBatchId", theirs.id())
                .get(BASE + "/fields/summary")
                .then()
                .statusCode(404)
                .body("code", Matchers.equalTo("BATCH_NOT_FOUND"));
        member().get(BASE + "/fields/summary")
                .then()
                .statusCode(200)
                .body("fields.phone.PENDING", Matchers.greaterThanOrEqualTo(2))
                .body("exportBatchState", Matchers.nullValue());
    }

    @Test
    void practitionerViewGroups() {
        String batch = batch(6);
        FieldRecommendation firstName = stage(doc(batch, "cert-g1", "1000000601", null, "first_name", STAGED));
        FieldRecommendation phone = stage(doc(batch, "cert-g1", "1000000601", "loc-1", "phone", STAGED));
        FieldRecommendation fax = stage(doc(batch, "cert-g1", "1000000601", "loc-2", "fax", STAGED));
        stage(doc(batch, "cert-g2", "1000000602", "loc-1", "phone", STAGED));

        member().queryParam("exportBatchId", batch)
                .get(BASE + "/practitioners/{id}", "cert-g1")
                .then()
                .statusCode(200)
                .body("practitioner.certifyPractitionerId", Matchers.equalTo("cert-g1"))
                .body("practitioner.npi", Matchers.equalTo("1000000601"))
                .body("exportBatchId", Matchers.equalTo(batch))
                .body("fields.id", Matchers.contains(firstName.id(), phone.id(), fax.id()))
                .body("fields[0].location", Matchers.nullValue())
                .body("fields[1].location.certifyLocationId", Matchers.equalTo("loc-1"));
        member().get(BASE + "/practitioners/{id}", "cert-nobody")
                .then()
                .statusCode(404)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("PRACTITIONER_NOT_FOUND"));
        member().queryParam("exportBatchId", batch(66))
                .get(BASE + "/practitioners/{id}", "cert-g1")
                .then()
                .statusCode(404)
                .body("code", Matchers.equalTo("PRACTITIONER_NOT_FOUND"));
    }

    @Test
    void npiViewWithoutBatchTakesTheLatest() {
        String september = Ids.batchId(TENANT, "candor", YearMonth.of(2026, 9), 7);
        String october = Ids.batchId(TENANT, "acme", YearMonth.of(2026, 10), 7);
        stage(doc(september, "cert-n1", "1000000701", "loc-1", "phone", STAGED));
        FieldRecommendation latest =
                stage(doc(october, "cert-n1", "1000000701", "loc-1", "fax", STAGED.plusSeconds(86_400)));

        member().get(BASE + "/npis/{npi}", "1000000701")
                .then()
                .statusCode(200)
                .body("practitioner.certifyPractitionerId", Matchers.equalTo("cert-n1"))
                .body("practitioner.npi", Matchers.equalTo("1000000701"))
                .body("exportBatchId", Matchers.equalTo(october))
                .body("fields.id", Matchers.contains(latest.id()));
        member().queryParam("exportBatchId", september)
                .get(BASE + "/npis/{npi}", "1000000701")
                .then()
                .statusCode(200)
                .body("exportBatchId", Matchers.equalTo(september))
                .body("fields", Matchers.hasSize(1))
                .body("fields[0].field", Matchers.equalTo("phone"));
        member().get(BASE + "/npis/{npi}", "1999999999")
                .then()
                .statusCode(404)
                .body("code", Matchers.equalTo("PRACTITIONER_NOT_FOUND"));
    }

    @Test
    void fieldTrailListsItsEvents() {
        String batch = batch(8);
        FieldRecommendation doc = stage(doc(batch, "cert-e1", "1000000801", "loc-1", "phone", STAGED));
        FieldRecommendation neighbour = stage(doc(batch, "cert-e1", "1000000801", "loc-1", "fax", STAGED));
        writeEvent(doc, AuditEventType.FIELD_SKIPPED, "PENDING", 2);
        writeEvent(neighbour, AuditEventType.FIELD_SKIPPED, "PENDING", 2);
        writeEvent(doc, AuditEventType.FIELD_APPROVED, "SKIPPED", 3);

        member().get(BASE + "/fields/{id}/events", doc.id())
                .then()
                .statusCode(200)
                .body("items.type", Matchers.contains("FIELD_SKIPPED", "FIELD_APPROVED"))
                .body("items[0].fieldRecommendationId", Matchers.equalTo(doc.id()))
                .body("items[0].certifyPractitionerId", Matchers.equalTo("cert-e1"))
                .body("items[0].uploadId", Matchers.equalTo(doc.sourceFile().uploadId()))
                .body("items[0].exportBatchId", Matchers.equalTo(batch))
                .body("items[0].actor", Matchers.equalTo(REVIEWER))
                .body("items[1].detail.fromStatus", Matchers.equalTo("SKIPPED"))
                .body("items[1].detail.version", Matchers.equalTo(3));
        FieldRecommendation quiet = stage(doc(batch, "cert-e2", "1000000802", "loc-2", "phone", STAGED));
        member().get(BASE + "/fields/{id}/events", quiet.id())
                .then()
                .statusCode(200)
                .body("items", Matchers.empty());
    }

    @Test
    void unknownFieldIs400() {
        member().queryParam("field", "nickname")
                .get(BASE + "/fields")
                .then()
                .statusCode(400)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("FIELD_UNKNOWN"));
        member().queryParam("reviewStatus", "BOGUS")
                .get(BASE + "/fields")
                .then()
                .statusCode(400)
                .body("code", Matchers.equalTo("INVALID_REQUEST"));
    }

    @Test
    void decisionIs200WithDocument() {
        String batch = batch(9);
        FieldRecommendation doc = stage(doc(batch, "cert-d1", "1000000901", "loc-1", "phone", STAGED));

        member().body("{\"action\":\"APPROVE\",\"version\":1}")
                .post(BASE + "/fields/{id}/decision", doc.id())
                .then()
                .statusCode(200)
                .body("id", Matchers.equalTo(doc.id()))
                .body("reviewStatus", Matchers.equalTo("APPROVED"))
                .body("version", Matchers.equalTo(2))
                .body("reviewHistory[0].action", Matchers.equalTo("APPROVE"))
                .body("reviewHistory[0].by", Matchers.equalTo(REVIEWER));
        member().body("{\"action\":\"APPROVE\",\"version\":1}")
                .post(BASE + "/fields/{id}/decision", doc.id())
                .then()
                .statusCode(409)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("DECISION_CONFLICT"))
                .body("detail", Matchers.containsString("\"reviewStatus\":\"APPROVED\""))
                .body("detail", Matchers.containsString("\"lastDecisionBy\":\"" + REVIEWER + "\""));
        member().get(BASE + "/fields/{id}/events", doc.id())
                .then()
                .statusCode(200)
                .body("items.type", Matchers.contains("FIELD_APPROVED"))
                .body("items[0].actor", Matchers.equalTo(REVIEWER));

        FieldRecommendation other = stage(doc(batch, "cert-d2", "1000000902", "loc-2", "phone", STAGED));
        member().body("{\"action\":\"REJECT\",\"version\":1}")
                .post(BASE + "/fields/{id}/decision", other.id())
                .then()
                .statusCode(400)
                .body("code", Matchers.equalTo("COMMENT_REQUIRED"));
        member().body("{\"action\":\"APPROVE\"}")
                .post(BASE + "/fields/{id}/decision", other.id())
                .then()
                .statusCode(400)
                .body("code", Matchers.equalTo("VERSION_REQUIRED"));
        member().body("{\"action\":\"ROLLBACK\",\"version\":1}")
                .post(BASE + "/fields/{id}/decision", other.id())
                .then()
                .statusCode(400)
                .body("code", Matchers.equalTo("INVALID_REQUEST"));
        member().body("{\"action\":\"MAYBE\",\"version\":1}")
                .post(BASE + "/fields/{id}/decision", other.id())
                .then()
                .statusCode(400)
                .body("code", Matchers.equalTo("INVALID_REQUEST"));
        member().post(BASE + "/fields/{id}/decision", other.id())
                .then()
                .statusCode(400)
                .body("code", Matchers.equalTo("INVALID_REQUEST"));
        member().body("{\"action\":\"APPROVE\",\"version\":1}")
                .post(BASE + "/fields/{id}/decision", "org-review|candor|nope|cert-x|-|phone")
                .then()
                .statusCode(404)
                .body("code", Matchers.equalTo("FIELD_NOT_FOUND"));
    }

    @Test
    void bulkIs207() {
        String batch = batch(10);
        FieldRecommendation approve = stage(doc(batch, "cert-u1", "1000001001", "loc-1", "phone", STAGED));
        FieldRecommendation reject = stage(doc(batch, "cert-u2", "1000001002", "loc-2", "phone", STAGED));
        String missing = Ids.fieldRecommendationId(TENANT, "candor", batch, "cert-u9", null, "phone");

        member().body("{\"decisions\":["
                        + "{\"id\":\"" + approve.id() + "\",\"action\":\"APPROVE\",\"version\":1},"
                        + "{\"id\":\"" + reject.id()
                        + "\",\"action\":\"REJECT\",\"comment\":\"still old\",\"version\":1},"
                        + "{\"id\":\"" + missing + "\",\"action\":\"APPROVE\",\"version\":1},"
                        + "{\"id\":\"" + approve.id() + "\",\"action\":\"SKIP\",\"version\":1},"
                        + "{\"id\":\"" + reject.id() + "\",\"action\":\"NOPE\",\"version\":2}]}")
                .post(BASE + "/decisions")
                .then()
                .statusCode(207)
                .body("results.id", Matchers.contains(approve.id(), reject.id(), missing, approve.id(), reject.id()))
                .body("results.status", Matchers.contains(200, 200, 404, 409, 400))
                .body("results[0].code", Matchers.nullValue())
                .body("results[0].document.reviewStatus", Matchers.equalTo("APPROVED"))
                .body("results[1].document.reviewStatus", Matchers.equalTo("REJECTED"))
                .body("results[2].code", Matchers.equalTo("FIELD_NOT_FOUND"))
                .body("results[2].document", Matchers.nullValue())
                .body("results[3].code", Matchers.equalTo("DECISION_CONFLICT"))
                .body("results[3].document.version", Matchers.equalTo(2))
                .body("results[4].code", Matchers.equalTo("INVALID_REQUEST"));

        StringBuilder tooMany = new StringBuilder("{\"decisions\":[");
        for (int index = 0; index < 201; index++) {
            tooMany.append(index == 0 ? "" : ",")
                    .append("{\"id\":\"")
                    .append(approve.id())
                    .append("\",\"action\":\"SKIP\",\"version\":2}");
        }
        member().body(tooMany.append("]}").toString())
                .post(BASE + "/decisions")
                .then()
                .statusCode(400)
                .body("code", Matchers.equalTo("INVALID_REQUEST"));
        member().body("{\"decisions\":[]}")
                .post(BASE + "/decisions")
                .then()
                .statusCode(400)
                .body("code", Matchers.equalTo("INVALID_REQUEST"));
        member().get(BASE + "/fields/{id}", approve.id())
                .then()
                .statusCode(200)
                .body("reviewStatus", Matchers.equalTo("APPROVED"))
                .body("version", Matchers.equalTo(2));
    }

    // helpers

    static String batch(int seq) {
        return Ids.batchId(TENANT, "candor", YearMonth.of(2026, 10), seq);
    }

    static FieldRecommendation doc(
            String batch, String practitionerId, String npi, String locationId, String field, Instant stagedAt) {
        return ReviewFixtures.pending(
                TENANT, batch, practitionerId, npi, locationId, field, "Greenwood Village", stagedAt);
    }

    FieldRecommendation stage(FieldRecommendation doc) {
        Assertions.assertEquals(
                Set.of(doc.id()), repository.upsertAll(List.of(doc)).insertedIds(), "fixture ids are unique");
        return doc;
    }

    /** Moves a document one step through the repository's guarded update, as a decision would. */
    void move(FieldRecommendation doc, ReviewStatus to) {
        Instant at = STAGED.plusSeconds(3600);
        boolean moved = transactions.run(session -> repository.decide(
                session,
                doc.tenantId(),
                doc.id(),
                doc.version(),
                Set.of(doc.reviewStatus()),
                to,
                Updates.push(
                        "reviewHistory",
                        new FieldRecommendation.HistoryEntry("FIXTURE", REVIEWER, at, null).toDocument()),
                at));
        Assertions.assertTrue(moved);
    }

    void writeEvent(FieldRecommendation doc, AuditEventType type, String fromStatus, long version) {
        AuditEvent.Builder builder = AuditEvent.of(type, doc.tenantId(), doc.source())
                .batch(doc.exportBatchId())
                .upload(doc.sourceFile().uploadId())
                .field(doc.id(), doc.practitioner().certifyPractitionerId())
                .actor(REVIEWER)
                .detail("field", doc.field())
                .detail("fromStatus", fromStatus)
                .detail("version", version);
        AuditEvent event = builder.build();
        transactions.run(session -> {
            audit.write(session, event);
            return null;
        });
    }

    ExportBatch deliveredBatch(String tenant, String vendor) {
        ExportBatch egressed = EgressFixtures.batchIn(
                transactions,
                batches,
                new ExportNpiRepository(collections),
                tenant,
                vendor,
                BatchState.EGRESS_COMPLETED,
                1,
                InboundFixtures.DELIVERED_AT);
        transactions.run(session -> batches.transition(
                session,
                egressed.id(),
                BatchState.EGRESS_COMPLETED,
                BatchState.DELIVERED,
                InboundFixtures.DELIVERED_AT.plusSeconds(60),
                null));
        return batches.find(egressed.id()).orElseThrow();
    }
}
