package com.certifyos.directory_accuracy.export.events;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.auth.PushIdentity;
import com.certifyos.directory_accuracy.auth.PushTokenRejectedException;
import com.certifyos.directory_accuracy.auth.PushTokenVerifier;
import com.certifyos.directory_accuracy.http.Problem;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/**
 * The push endpoint through its filter. The Google verifier is replaced by a mock: there are no
 * Google certificates to fetch in CI, and the real verifier has its own unit test.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
class EgressEventResourceIT {

    static final String PATH = "/internal/vendor-exports/egress-events";

    @InjectMock
    PushTokenVerifier verifier;

    @BeforeEach
    void before() {
        Mockito.when(verifier.verify("good")).thenReturn(new PushIdentity("pubsub-push@test.iam.gserviceaccount.com"));
        Mockito.when(verifier.verify(ArgumentMatchers.argThat(token -> !"good".equals(token))))
                .thenThrow(new PushTokenRejectedException("token email is not the push service account"));
    }

    private static String envelope(String eventJson) {
        String data = Base64.getEncoder().encodeToString(eventJson.getBytes(StandardCharsets.UTF_8));
        return "{\"message\":{\"data\":\"" + data
                + "\",\"messageId\":\"m-42\",\"publishTime\":\"2026-10-01T07:12:01Z\","
                + "\"attributes\":{\"initiator\":\"vendor-exchange-worker\"}},"
                + "\"subscription\":\"projects/p/subscriptions/vendor-exchange-egress-events\"}";
    }

    static final String EVENT = "{\"schemaVersion\":\"1\",\"type\":\"EXPORT_COMPLETED\",\"tenantId\":\"org-a\","
            + "\"correlationId\":\"org-a-candor-2026-10-001-r1\",\"phase\":\"COMPLETED\","
            + "\"outputUri\":\"gs://b/from/f.csv\",\"totalRecords\":1,\"totalRows\":1,\"completedAt\":\"2026-10-01T07:12:00Z\"}";

    @Test
    void verifiedPushIsAcknowledged() {
        RestAssured.given()
                .header("Authorization", "Bearer good")
                .contentType("application/json")
                .body(envelope(EVENT))
                .post(PATH)
                .then()
                .statusCode(204);
    }

    @Test
    void malformedEventIsAcknowledgedAndDropped() {
        RestAssured.given()
                .header("Authorization", "Bearer good")
                .contentType("application/json")
                .body(envelope("not json at all"))
                .post(PATH)
                .then()
                .statusCode(204);
    }

    @Test
    void missingTokenIs401Problem() {
        RestAssured.given()
                .contentType("application/json")
                .body(envelope(EVENT))
                .post(PATH)
                .then()
                .statusCode(401)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("PUSH_TOKEN_REQUIRED"));
    }

    @Test
    void rejectedTokenIs401Problem() {
        RestAssured.given()
                .header("Authorization", "Bearer forged")
                .contentType("application/json")
                .body(envelope(EVENT))
                .post(PATH)
                .then()
                .statusCode(401)
                .body("code", Matchers.equalTo("PUSH_TOKEN_REJECTED"))
                .body("detail", Matchers.equalTo("token email is not the push service account"));
    }

    @Test
    void pushEndpointNeedsNoPlatformTokenOrTenantHeader() {
        // No @TestSecurity and no tenant-id: the /internal policy permits, the push filter guards.
        RestAssured.given()
                .header("Authorization", "Bearer good")
                .contentType("application/json")
                .body(envelope(EVENT))
                .post(PATH)
                .then()
                .statusCode(204);
    }
}
