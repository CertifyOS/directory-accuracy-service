package com.certifyos.directory_accuracy.auth;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.http.Problem;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.http.Fault;
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

/**
 * The tenant-membership chain against a WireMock DAL. The HTTP-layer policy (a platform token for
 * every {@code /v1/*} call) is framework enforcement that a Quarkus test cannot exercise without a
 * live OIDC tenant; it is proven by the deployed smoke test.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockDal.class)
@TestProfile(ApiTestProfile.class)
class UserContextFilterIT {

    static final String EMAIL = UserContextFilter.EMAIL_CLAIM;

    // The real token service would reach for Application Default Credentials, which CI does not have.
    @InjectMock
    GoogleIdTokenService tokens;

    @BeforeEach
    void before() {
        Mockito.when(tokens.idToken("test-dal-iap-client-id")).thenReturn("dal-id-token");
        WireMockDal.stubMember("dev@certifyos.com", "org-a");
        WireMockDal.stubFor(WireMock.get(WireMock.urlPathEqualTo("/users/by-email"))
                .withQueryParam("email", WireMock.equalTo("ghost@certifyos.com"))
                .willReturn(WireMock.aResponse().withStatus(404)));
    }

    private static RequestSpecification probe(String tenant) {
        RequestSpecification spec = RestAssured.given();
        return tenant == null ? spec : spec.header("tenant-id", tenant);
    }

    @Test
    @TestSecurity(user = "dev")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "dev@certifyos.com")})
    void memberPassesAndTheContextIsFilled() {
        probe("org-a")
                .get("/v1/_probe")
                .then()
                .statusCode(200)
                .body("email", Matchers.equalTo("dev@certifyos.com"))
                .body("tenantId", Matchers.equalTo("org-a"))
                .body("permissions.org-a.vendor-export", Matchers.contains("read", "manage"));
    }

    @Test
    @TestSecurity(user = "dev")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "dev@certifyos.com")})
    void noRoleInTheTenantIs403() {
        probe("org-b")
                .get("/v1/_probe")
                .then()
                .statusCode(403)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("TENANT_FORBIDDEN"))
                .body("instance", Matchers.equalTo("/v1/_probe"));
    }

    @Test
    @TestSecurity(user = "dev")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "dev@certifyos.com")})
    void missingTenantHeaderIs400() {
        probe(null)
                .get("/v1/_probe")
                .then()
                .statusCode(400)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("TENANT_REQUIRED"));
    }

    @Test
    @TestSecurity(user = "m2m")
    void tokenWithoutEmailIs403() {
        probe("org-a").get("/v1/_probe").then().statusCode(403).body("code", Matchers.equalTo("TENANT_UNRESOLVED"));
    }

    @Test
    @TestSecurity(user = "ghost")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "ghost@certifyos.com")})
    void unknownUserIs403AndNeverCached() {
        probe("org-a").get("/v1/_probe").then().statusCode(403).body("code", Matchers.equalTo("TENANT_FORBIDDEN"));
        probe("org-a").get("/v1/_probe").then().statusCode(403);
        WireMockDal.verify(
                2,
                WireMock.getRequestedFor(WireMock.urlPathEqualTo("/users/by-email"))
                        .withQueryParam("email", WireMock.equalTo("ghost@certifyos.com")));
    }

    @Test
    @TestSecurity(user = "down")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "down@certifyos.com")})
    void dalDownIs503() {
        WireMockDal.stubFor(WireMock.get(WireMock.urlPathEqualTo("/users/by-email"))
                .withQueryParam("email", WireMock.equalTo("down@certifyos.com"))
                .willReturn(WireMock.aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER)));
        probe("org-a")
                .get("/v1/_probe")
                .then()
                .statusCode(503)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("DAL_UNAVAILABLE"));
    }

    @Test
    @TestSecurity(user = "hdr")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "hdr@certifyos.com")})
    void lookupCarriesTheHeadersTheDalInterceptorRequires() {
        WireMockDal.stubMember("hdr@certifyos.com", "org-h");
        probe("org-h").get("/v1/_probe").then().statusCode(200);
        WireMockDal.verify(
                1,
                WireMock.getRequestedFor(WireMock.urlPathEqualTo("/users/by-email"))
                        .withQueryParam("email", WireMock.equalTo("hdr@certifyos.com"))
                        .withQueryParam("includePermissions", WireMock.equalTo("true"))
                        .withHeader("Authorization", WireMock.equalTo("Bearer dal-id-token"))
                        .withHeader("requesting-user-id", WireMock.equalTo("vendor-exchange-service"))
                        .withHeader("requesting-organization-id", WireMock.equalTo("org-h")));
    }

    @Test
    @TestSecurity(user = "cached")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "cached@certifyos.com")})
    void successfulLookupIsCachedAcrossTenants() {
        WireMockDal.stubMember("cached@certifyos.com", "org-c");
        probe("org-c").get("/v1/_probe").then().statusCode(200);
        probe("org-c").get("/v1/_probe").then().statusCode(200);
        probe("org-other").get("/v1/_probe").then().statusCode(403);
        WireMockDal.verify(
                1,
                WireMock.getRequestedFor(WireMock.urlPathEqualTo("/users/by-email"))
                        .withQueryParam("email", WireMock.equalTo("cached@certifyos.com")));
    }
}
