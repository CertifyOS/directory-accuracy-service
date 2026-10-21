package com.certifyos.vendor_exchange.ingestion.api;

import com.certifyos.vendor_exchange.ApiTestProfile;
import com.certifyos.vendor_exchange.MongoResource;
import com.certifyos.vendor_exchange.auth.GoogleIdTokenService;
import com.certifyos.vendor_exchange.auth.UserContextFilter;
import com.certifyos.vendor_exchange.auth.WireMockDal;
import com.certifyos.vendor_exchange.http.Problem;
import com.certifyos.vendor_exchange.ingestion.release.ReleaseFixtures;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendationRepository;
import com.certifyos.vendor_exchange.ingestion.staging.ReviewStatus;
import com.certifyos.vendor_exchange.persistence.Collections;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.security.TestSecurity;
import io.quarkus.test.security.jwt.Claim;
import io.quarkus.test.security.jwt.JwtSecurity;
import io.restassured.RestAssured;
import io.restassured.specification.RequestSpecification;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The release endpoints under {@code /v1/vendor-review}: single release, bulk release by ids or by
 * batch, the release read and rollback. The tenant is the caller's; another tenant's field or
 * release is 404. Unique email and tenants for this class (finding 72).
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockDal.class)
@TestProfile(ApiTestProfile.class)
class VendorReleaseResourceIT {

    static final String EMAIL = UserContextFilter.EMAIL_CLAIM;
    static final String REVIEWER = "relres@certifyos.com";
    static final String TENANT = "org-relres";
    static final String OTHER = "org-relres-other";
    static final String BASE = "/v1/vendor-review";

    @Inject
    Collections collections;

    @Inject
    FieldRecommendationRepository repository;

    @InjectMock
    GoogleIdTokenService google;

    @BeforeEach
    void before() {
        Mockito.when(google.idToken("test-dal-iap-client-id")).thenReturn("dal-id-token");
        WireMockDal.stubMember(REVIEWER, TENANT);
        ReleaseFixtures.stubRanking(TENANT, true);
    }

    private static RequestSpecification member() {
        return RestAssured.given().header("tenant-id", TENANT).contentType("application/json");
    }

    private FieldRecommendation seed(String tenant, String practitionerId, String field, String locationId) {
        return ReleaseFixtures.insert(
                        collections,
                        ReleaseFixtures.approved(
                                tenant, practitionerId, "1003002890", locationId, field, "value-" + field))
                .get(0);
    }

    @Test
    @TestSecurity(user = "rev")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = REVIEWER)})
    void releaseFieldIs202AndTheReleaseReadsBack() {
        FieldRecommendation doc = seed(TENANT, "cert-single", "first_name", null);

        String releaseId = member().body(Map.of("version", 2))
                .post(BASE + "/fields/{id}/release", doc.id())
                .then()
                .statusCode(202)
                .body("releaseId", Matchers.startsWith("rel-"))
                .extract()
                .path("releaseId");

        member().get(BASE + "/releases/{releaseId}", releaseId)
                .then()
                .statusCode(200)
                .body("releaseId", Matchers.equalTo(releaseId))
                .body("fields", Matchers.hasSize(1))
                .body("fields[0].id", Matchers.equalTo(doc.id()))
                .body("fields[0].reviewStatus", Matchers.equalTo("RELEASE_REQUESTED"))
                .body("fields[0].version", Matchers.equalTo(3))
                .body("fields[0].goldenRecordUpdate.releaseId", Matchers.equalTo(releaseId))
                .body("fields[0].goldenRecordUpdate.requestedBy", Matchers.equalTo(REVIEWER))
                .body("fields[0].practitioner.certifyPractitionerId", Matchers.equalTo("cert-single"));
        member().get(BASE + "/releases/{releaseId}", "rel-none")
                .then()
                .statusCode(404)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("RELEASE_NOT_FOUND"));
    }

    @Test
    @TestSecurity(user = "rev")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = REVIEWER)})
    void releaseFieldRefusals() {
        FieldRecommendation doc = seed(TENANT, "cert-refusals", "phone", "loc-1");
        FieldRecommendation inPlace = seed(TENANT, "cert-refusals", "practice_name", "loc-1");

        member().body(Map.of())
                .post(BASE + "/fields/{id}/release", doc.id())
                .then()
                .statusCode(400)
                .body("code", Matchers.equalTo("VERSION_REQUIRED"));
        member().body(Map.of("version", 1))
                .post(BASE + "/fields/{id}/release", doc.id())
                .then()
                .statusCode(409)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("RELEASE_NOT_ALLOWED"));
        member().body(Map.of("version", 2))
                .post(BASE + "/fields/{id}/release", inPlace.id())
                .then()
                .statusCode(422)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("IN_PLACE_RELEASE_UNSUPPORTED"));
        Assertions.assertEquals(
                ReviewStatus.APPROVED,
                repository.find(TENANT, doc.id()).orElseThrow().reviewStatus());
    }

    @Test
    @TestSecurity(user = "rev")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = REVIEWER)})
    void bulkByIdsIs202WithReleasesAndSkipped() {
        FieldRecommendation first = seed(TENANT, "cert-bulk", "first_name", null);
        FieldRecommendation last = seed(TENANT, "cert-bulk", "last_name", null);
        FieldRecommendation inPlace = seed(TENANT, "cert-bulk", "specialties", null);

        member().body(Map.of(
                        "ids",
                        List.of(first.id(), last.id(), inPlace.id()),
                        "versions",
                        Map.of(first.id(), 2, last.id(), 2, inPlace.id(), 2)))
                .post(BASE + "/releases")
                .then()
                .statusCode(202)
                .body("releases", Matchers.hasSize(1))
                .body("releases[0].certifyPractitionerId", Matchers.equalTo("cert-bulk"))
                .body("releases[0].fieldRecommendationIds", Matchers.contains(first.id(), last.id()))
                .body("skipped", Matchers.hasSize(1))
                .body("skipped[0].id", Matchers.equalTo(inPlace.id()))
                .body("skipped[0].code", Matchers.equalTo("IN_PLACE_RELEASE_UNSUPPORTED"));
    }

    @Test
    @TestSecurity(user = "rev")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = REVIEWER)})
    void bulkByBatchIs202() {
        FieldRecommendation fax = seed(TENANT, "cert-batch", "fax", "loc-2");

        member().body(Map.of("exportBatchId", ReleaseFixtures.batchId(TENANT), "all", true))
                .post(BASE + "/releases")
                .then()
                .statusCode(202)
                .body("releases.fieldRecommendationIds.flatten()", Matchers.hasItem(fax.id()))
                // other tests of the class leave approved in-place fields in the batch; only those are listed
                .body("skipped.code", Matchers.everyItem(Matchers.equalTo("IN_PLACE_RELEASE_UNSUPPORTED")));
        Assertions.assertEquals(
                ReviewStatus.RELEASE_REQUESTED,
                repository.find(TENANT, fax.id()).orElseThrow().reviewStatus());
    }

    @Test
    @TestSecurity(user = "rev")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = REVIEWER)})
    void bulkBodyIsValidated() {
        List<String> tooMany = new ArrayList<>();
        for (int index = 0; index <= 5000; index++) {
            tooMany.add("id-" + index);
        }
        for (Object body : List.of(
                Map.of(),
                Map.of("ids", List.of()),
                Map.of("exportBatchId", "b-1"),
                Map.of("exportBatchId", "b-1", "all", true, "ids", List.of("x")),
                Map.of("ids", tooMany, "versions", Map.of()))) {
            member().body(body)
                    .post(BASE + "/releases")
                    .then()
                    .statusCode(400)
                    .contentType(Problem.MEDIA_TYPE)
                    .body("code", Matchers.equalTo("INVALID_REQUEST"));
        }
    }

    @Test
    @TestSecurity(user = "rev")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = REVIEWER)})
    void rollbackIs200WithTheField() {
        FieldRecommendation doc = seed(TENANT, "cert-rollback", "last_name", null);

        member().body(Map.of("version", 2, "reason", "wrong practitioner"))
                .post(BASE + "/fields/{id}/rollback", doc.id())
                .then()
                .statusCode(200)
                .body("id", Matchers.equalTo(doc.id()))
                .body("reviewStatus", Matchers.equalTo("PENDING"))
                .body("version", Matchers.equalTo(3))
                .body("reviewHistory[-1].action", Matchers.equalTo("ROLLBACK"))
                .body("reviewHistory[-1].comment", Matchers.equalTo("wrong practitioner"));
        member().body(Map.of("version", 3, "reason", "again"))
                .post(BASE + "/fields/{id}/rollback", doc.id())
                .then()
                .statusCode(409)
                .body("code", Matchers.equalTo("ROLLBACK_NOT_ALLOWED"));
        member().body(Map.of("reason", "no version"))
                .post(BASE + "/fields/{id}/rollback", doc.id())
                .then()
                .statusCode(400)
                .body("code", Matchers.equalTo("VERSION_REQUIRED"));
    }

    @Test
    @TestSecurity(user = "rev")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = REVIEWER)})
    void anotherTenantsFieldOrReleaseIs404() {
        ReleaseFixtures.stubRanking(OTHER, true);
        FieldRecommendation theirs = seed(OTHER, "cert-theirs", "first_name", null);
        ReleaseFixtures.insert(
                collections,
                ReleaseFixtures.field(
                        OTHER,
                        "cert-theirs",
                        "1003002890",
                        null,
                        "last_name",
                        "Lee",
                        ReviewStatus.RELEASE_REQUESTED,
                        3L,
                        ReleaseFixtures.requested("rel-theirs", "CorePractitioner")));

        member().body(Map.of("version", 2))
                .post(BASE + "/fields/{id}/release", theirs.id())
                .then()
                .statusCode(404)
                .body("code", Matchers.equalTo("FIELD_NOT_FOUND"));
        member().body(Map.of("version", 2, "reason", "x"))
                .post(BASE + "/fields/{id}/rollback", theirs.id())
                .then()
                .statusCode(404)
                .body("code", Matchers.equalTo("FIELD_NOT_FOUND"));
        member().get(BASE + "/releases/{releaseId}", "rel-theirs")
                .then()
                .statusCode(404)
                .body("code", Matchers.equalTo("RELEASE_NOT_FOUND"));
        member().body(Map.of("ids", List.of(theirs.id()), "versions", Map.of(theirs.id(), 2)))
                .post(BASE + "/releases")
                .then()
                .statusCode(202)
                .body("releases", Matchers.empty())
                .body("skipped[0].code", Matchers.equalTo("FIELD_NOT_FOUND"));
        Assertions.assertEquals(
                ReviewStatus.APPROVED,
                repository.find(OTHER, theirs.id()).orElseThrow().reviewStatus());
    }

    @Test
    @TestSecurity(user = "rev")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = REVIEWER)})
    void anotherResourceMayShareTheReviewRootPath() {
        // The review read endpoints (another step) live in their own class under /v1/vendor-review; the
        // probe stands in for it and shows two root resources on one path both resolve.
        member().get(BASE + "/path-probe").then().statusCode(204);
        member().get(BASE + "/releases/{releaseId}", "rel-probe-none").then().statusCode(404);
    }
}
