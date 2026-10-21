package com.certifyos.directory_accuracy.ingestion.events;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.auth.PushIdentity;
import com.certifyos.directory_accuracy.auth.PushTokenRejectedException;
import com.certifyos.directory_accuracy.auth.PushTokenVerifier;
import com.certifyos.directory_accuracy.export.events.PubSubPushMessage;
import com.certifyos.directory_accuracy.http.Problem;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import jakarta.inject.Inject;
import java.util.Map;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/** The ov-generated push endpoint through its filter; the Google verifier is a mock. */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
class OvGeneratedResourceIT {

    static final String PATH = "/internal/vendor-ingestion/ov-generated";

    @InjectMock
    PushTokenVerifier verifier;

    @Inject
    ObjectMapper mapper;

    @BeforeEach
    void before() {
        Mockito.when(verifier.verify("good")).thenReturn(new PushIdentity("pubsub-push@test.iam.gserviceaccount.com"));
        Mockito.when(verifier.verify(ArgumentMatchers.argThat(token -> !"good".equals(token))))
                .thenThrow(new PushTokenRejectedException("token email is not the push service account"));
    }

    private String envelope(PubSubPushMessage.Message message) throws Exception {
        return mapper.writeValueAsString(
                Map.of("message", message, "subscription", "projects/p/subscriptions/vendor-ingestion-ov-generated"));
    }

    @Test
    void verifiedPushIs204() throws Exception {
        RestAssured.given()
                .header("Authorization", "Bearer good")
                .contentType("application/json")
                .body(envelope(OvGeneratedHandlerIT.message("ovres-t", "cert-1", "practitioner", "roster:ovres-t")))
                .post(PATH)
                .then()
                .statusCode(204);
        RestAssured.given()
                .header("Authorization", "Bearer good")
                .contentType("application/json")
                .body("{\"subscription\": \"projects/p/subscriptions/vendor-ingestion-ov-generated\"}")
                .post(PATH)
                .then()
                .statusCode(204);
    }

    @Test
    void missingTokenIs401Problem() throws Exception {
        RestAssured.given()
                .contentType("application/json")
                .body(envelope(OvGeneratedHandlerIT.message("ovres-t", "cert-1", "practitioner", "candor:ovres-t")))
                .post(PATH)
                .then()
                .statusCode(401)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("PUSH_TOKEN_REQUIRED"));
    }

    @Test
    void rejectedTokenIs401Problem() throws Exception {
        RestAssured.given()
                .header("Authorization", "Bearer forged")
                .contentType("application/json")
                .body(envelope(OvGeneratedHandlerIT.message("ovres-t", "cert-1", "practitioner", "candor:ovres-t")))
                .post(PATH)
                .then()
                .statusCode(401)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("PUSH_TOKEN_REJECTED"));
    }
}
