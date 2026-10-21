package com.certifyos.directory_accuracy.export.api;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.certifyos.directory_accuracy.auth.UserContextFilter;
import com.certifyos.directory_accuracy.auth.WireMockDal;
import com.certifyos.directory_accuracy.http.Problem;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.security.TestSecurity;
import io.quarkus.test.security.jwt.Claim;
import io.quarkus.test.security.jwt.JwtSecurity;
import io.restassured.RestAssured;
import io.restassured.specification.RequestSpecification;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockDal.class)
@TestProfile(ApiTestProfile.class)
class VendorExportsResourceIT {

    static final String EMAIL = UserContextFilter.EMAIL_CLAIM;

    @InjectMock
    GoogleIdTokenService tokens;

    @BeforeEach
    void before() {
        Mockito.when(tokens.idToken("test-dal-iap-client-id")).thenReturn("dal-id-token");
        WireMockDal.stubMember("ops@certifyos.com", "org-ops");
    }

    private static RequestSpecification member() {
        return RestAssured.given().header("tenant-id", "org-ops");
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "ops@certifyos.com")})
    void schedulesListIsAnEmptyItemsAnswer() {
        member().get("/v1/vendor-exports/schedules")
                .then()
                .statusCode(200)
                .contentType("application/json")
                .body("items", Matchers.empty());
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "ops@certifyos.com")})
    void batchesListIsAnEmptyItemsAnswer() {
        member().get("/v1/vendor-exports").then().statusCode(200).body("items", Matchers.empty());
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "ops@certifyos.com")})
    void tickRunsInlineAndReportsItsCounts() {
        member().post("/v1/vendor-exports/tick")
                .then()
                .statusCode(200)
                .body("tickId", Matchers.notNullValue())
                .body("schedulesDue", Matchers.greaterThanOrEqualTo(0))
                .body("batchesCreated", Matchers.instanceOf(java.util.List.class))
                .body("skippedAlreadyExists", Matchers.greaterThanOrEqualTo(0))
                .body("durationMs", Matchers.greaterThanOrEqualTo(0));
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "ops@certifyos.com")})
    void unknownPathIsAProblem() {
        member().get("/v1/vendor-exports/nothing/here/at-all")
                .then()
                .statusCode(404)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("NOT_FOUND"))
                .body("type", Matchers.equalTo("urn:certifyos:vendor-exchange:problem:not-found"));
    }

    @Test
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "ops@certifyos.com")})
    void wrongMethodIsAProblem() {
        member().delete("/v1/vendor-exports/schedules")
                .then()
                .statusCode(405)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("METHOD_NOT_ALLOWED"));
    }

    @Test
    void openApiDocumentListsEveryEndpoint() {
        RestAssured.given()
                .get("/q/openapi?format=json")
                .then()
                .statusCode(200)
                .body("info.title", Matchers.equalTo("vendor-exchange-service"))
                .body("paths", Matchers.hasKey("/v1/vendor-exports"))
                .body("paths", Matchers.hasKey("/v1/vendor-exports/schedules"))
                .body("paths", Matchers.hasKey("/v1/vendor-exports/tick"))
                .body("paths", Matchers.hasKey("/internal/vendor-exports/egress-events"))
                .body("components.securitySchemes", Matchers.hasKey("platform-token"));
    }
}
