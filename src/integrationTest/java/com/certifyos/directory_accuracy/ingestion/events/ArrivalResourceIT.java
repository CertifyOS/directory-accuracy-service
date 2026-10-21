package com.certifyos.directory_accuracy.ingestion.events;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.auth.PushIdentity;
import com.certifyos.directory_accuracy.auth.PushTokenRejectedException;
import com.certifyos.directory_accuracy.auth.PushTokenVerifier;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.ingestion.InboundFixtures;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileRepository;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileState;
import com.certifyos.directory_accuracy.metrics.VendorExchangeMetrics;
import com.certifyos.directory_accuracy.persistence.Collections;
import com.certifyos.directory_accuracy.persistence.Ids;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.Map;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/**
 * The arrival push endpoint through its filter. The Google verifier is replaced by a mock, as in
 * the egress event endpoint's test.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
class ArrivalResourceIT {

    static final String PATH = "/internal/vendor-ingestion/arrivals";

    @InjectMock
    PushTokenVerifier verifier;

    @Inject
    ExportBatchRepository batches;

    @Inject
    InboundFileRepository inbound;

    @Inject
    Transactions transactions;

    @Inject
    Collections collections;

    @Inject
    MeterRegistry registry;

    @Inject
    ObjectMapper mapper;

    @BeforeEach
    void before() {
        Mockito.when(verifier.verify("good")).thenReturn(new PushIdentity("pubsub-push@test.iam.gserviceaccount.com"));
        Mockito.when(verifier.verify(ArgumentMatchers.argThat(token -> !"good".equals(token))))
                .thenThrow(new PushTokenRejectedException("token email is not the push service account"));
    }

    private String envelope(Map<String, String> attributes) throws Exception {
        Map<String, Object> message = new LinkedHashMap<>();
        message.put("messageId", "m-arrival");
        message.put("publishTime", "2026-10-15T09:12:42Z");
        if (attributes != null) {
            message.put("attributes", attributes);
        }
        return mapper.writeValueAsString(
                Map.of("message", message, "subscription", "projects/p/subscriptions/vendor-ingestion-arrivals"));
    }

    @Test
    void verifiedPushIs204() throws Exception {
        String tenant = "arrival-http";
        ExportBatch batch = InboundFixtures.deliveredBatch(transactions, batches, collections, tenant);
        String objectName = InboundFixtures.objectName(tenant, batch.id());

        RestAssured.given()
                .header("Authorization", "Bearer good")
                .contentType("application/json")
                .body(envelope(ArrivalHandlerIT.attributes(objectName, "9001", "OBJECT_FINALIZE")))
                .post(PATH)
                .then()
                .statusCode(204);

        Assertions.assertEquals(
                InboundFileState.RECEIVED,
                inbound.findByUploadId(Ids.uploadId(InboundFixtures.VENDOR_BUCKET, objectName, "9001"))
                        .orElseThrow()
                        .entry()
                        .state());
    }

    @Test
    void missingTokenIs401Problem() throws Exception {
        RestAssured.given()
                .contentType("application/json")
                .body(envelope(ArrivalHandlerIT.attributes("to/x/y.csv", "9101", "OBJECT_FINALIZE")))
                .post(PATH)
                .then()
                .statusCode(401)
                .contentType(com.certifyos.directory_accuracy.http.Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("PUSH_TOKEN_REQUIRED"));
    }

    @Test
    void rejectedTokenIs401Problem() throws Exception {
        RestAssured.given()
                .header("Authorization", "Bearer forged")
                .contentType("application/json")
                .body(envelope(ArrivalHandlerIT.attributes("to/x/y.csv", "9201", "OBJECT_FINALIZE")))
                .post(PATH)
                .then()
                .statusCode(401)
                .contentType(com.certifyos.directory_accuracy.http.Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("PUSH_TOKEN_REJECTED"));
    }

    @Test
    void envelopeWithoutAttributesIs204() throws Exception {
        double before = ignored("UNREADABLE");

        RestAssured.given()
                .header("Authorization", "Bearer good")
                .contentType("application/json")
                .body(envelope(null))
                .post(PATH)
                .then()
                .statusCode(204);
        RestAssured.given()
                .header("Authorization", "Bearer good")
                .contentType("application/json")
                .body("{\"subscription\":\"projects/p/subscriptions/vendor-ingestion-arrivals\"}")
                .post(PATH)
                .then()
                .statusCode(204);

        Assertions.assertEquals(
                before + 1, ignored("UNREADABLE"), "the message reached the handler; the empty envelope did not");
    }

    private double ignored(String eventType) {
        Counter found = registry.find(VendorExchangeMetrics.INGESTION_ARRIVALS_IGNORED_EVENT_TYPE)
                .tag("eventType", eventType)
                .counter();
        return found == null ? 0 : found.count();
    }
}
