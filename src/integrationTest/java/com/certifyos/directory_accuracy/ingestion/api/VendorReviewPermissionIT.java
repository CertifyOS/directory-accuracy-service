package com.certifyos.directory_accuracy.ingestion.api;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.certifyos.directory_accuracy.auth.UserContextFilter;
import com.certifyos.directory_accuracy.auth.WireMockDal;
import com.certifyos.directory_accuracy.http.Problem;
import com.certifyos.directory_accuracy.ingestion.ReviewFixtures;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendation;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendationRepository;
import com.certifyos.directory_accuracy.ingestion.staging.ReviewStatus;
import com.certifyos.directory_accuracy.persistence.Ids;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.security.TestSecurity;
import io.quarkus.test.security.jwt.Claim;
import io.quarkus.test.security.jwt.JwtSecurity;
import io.restassured.RestAssured;
import jakarta.inject.Inject;
import java.time.YearMonth;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The review endpoints with {@code vendor-exchange.permissions.enforce} on: a caller holding only
 * {@code vendor-review: [read]} reads but cannot decide. Its own profile, so Quarkus restarts once for
 * this class; a nested class cannot carry a second profile.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockDal.class)
@TestProfile(VendorReviewPermissionIT.Enforced.class)
@TestSecurity(user = "reader")
@JwtSecurity(claims = {@Claim(key = UserContextFilter.EMAIL_CLAIM, value = VendorReviewPermissionIT.READER)})
class VendorReviewPermissionIT {

    static final String READER = "review-reader@certifyos.com";
    static final String TENANT = "org-review-perm";
    static final String BASE = "/v1/vendor-review";

    /** The api profile with the permission check on. */
    public static class Enforced extends ApiTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> overrides = new HashMap<>(super.getConfigOverrides());
            overrides.put("vendor-exchange.permissions.enforce", "true");
            return overrides;
        }
    }

    @Inject
    FieldRecommendationRepository repository;

    @InjectMock
    GoogleIdTokenService google;

    @BeforeEach
    void before() {
        Mockito.when(google.idToken("test-dal-iap-client-id")).thenReturn("dal-id-token");
        WireMockDal.stubFor(WireMock.get(WireMock.urlPathEqualTo("/users/by-email"))
                .withQueryParam("email", WireMock.equalTo(READER))
                .willReturn(WireMock.okJson(
                        WireMockDal.userJson("u-reader", READER, TENANT, "vendor-review", "\"read\""))));
    }

    @Test
    void decidePermissionEnforcedWhenFlagOn() {
        FieldRecommendation doc = ReviewFixtures.pending(
                TENANT,
                Ids.batchId(TENANT, "candor", YearMonth.of(2026, 10), 1),
                "cert-perm",
                "1000009001",
                "loc-1",
                "phone",
                "(303) 679-0177",
                ReviewFixtures.STAGED);
        repository.upsertAll(List.of(doc));

        RestAssured.given()
                .header("tenant-id", TENANT)
                .get(BASE + "/fields/{id}", doc.id())
                .then()
                .statusCode(200)
                .body("reviewStatus", Matchers.equalTo("PENDING"));
        RestAssured.given()
                .header("tenant-id", TENANT)
                .contentType("application/json")
                .body("{\"action\":\"APPROVE\",\"version\":1}")
                .post(BASE + "/fields/{id}/decision", doc.id())
                .then()
                .statusCode(403)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("PERMISSION_DENIED"))
                .body("detail", Matchers.equalTo("requires vendor-review:decide"));
        RestAssured.given()
                .header("tenant-id", TENANT)
                .contentType("application/json")
                .body("{\"decisions\":[{\"id\":\"" + doc.id() + "\",\"action\":\"APPROVE\",\"version\":1}]}")
                .post(BASE + "/decisions")
                .then()
                .statusCode(403)
                .body("code", Matchers.equalTo("PERMISSION_DENIED"));
        Assertions.assertEquals(
                ReviewStatus.PENDING,
                repository.find(TENANT, doc.id()).orElseThrow().reviewStatus(),
                "a refused decision writes nothing");
    }
}
