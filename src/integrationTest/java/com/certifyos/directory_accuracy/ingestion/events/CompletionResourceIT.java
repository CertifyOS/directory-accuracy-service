package com.certifyos.directory_accuracy.ingestion.events;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.auth.PushIdentity;
import com.certifyos.directory_accuracy.auth.PushTokenRejectedException;
import com.certifyos.directory_accuracy.auth.PushTokenVerifier;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.export.events.PubSubPushMessage;
import com.certifyos.directory_accuracy.http.Problem;
import com.certifyos.directory_accuracy.ingestion.InboundFixtures;
import com.certifyos.directory_accuracy.ingestion.InboundFixtures.Seeded;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileRepository;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileState;
import com.certifyos.directory_accuracy.persistence.Collections;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
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
 * The completion push endpoint through its filter, with the Google verifier mocked as in the
 * arrival endpoint's test. Every tenant here starts with {@code complhttp-}.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
class CompletionResourceIT {

    static final String PATH = "/internal/vendor-ingestion/completions";

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
    ObjectMapper mapper;

    @BeforeEach
    void before() {
        Mockito.when(verifier.verify("good")).thenReturn(new PushIdentity("pubsub-push@test.iam.gserviceaccount.com"));
        Mockito.when(verifier.verify(ArgumentMatchers.argThat(token -> !"good".equals(token))))
                .thenThrow(new PushTokenRejectedException("token email is not the push service account"));
    }

    private Seeded seed(String tenant, InboundFileState state) {
        Seeded seeded = InboundFixtures.deliveredBatchWithEntry(transactions, batches, collections, tenant, state);
        collections
                .batches()
                .updateOne(
                        Filters.eq("_id", seeded.batch().id()),
                        Updates.combine(
                                Updates.set("inboundFiles.0.externalRef", CompletionHandlerIT.externalRef(seeded)),
                                Updates.set("inboundFiles.0.ingestionId", CompletionHandlerIT.ingestionId(tenant))));
        return seeded;
    }

    private String envelope(Seeded seeded, String tenant) throws Exception {
        PubSubPushMessage.Message message = CompletionHandlerIT.message(
                "m-http",
                CompletionHandlerIT.parsedBody(tenant, CompletionHandlerIT.externalRef(seeded), seeded, "COMPLETED"));
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("message", message);
        body.put("subscription", "projects/p/subscriptions/vendor-ingestion-completions");
        return mapper.writeValueAsString(body);
    }

    private InboundFileState state(Seeded seeded) {
        return inbound.findByUploadId(seeded.entry().uploadId())
                .orElseThrow()
                .entry()
                .state();
    }

    @Test
    void verifiedPushIs204() throws Exception {
        String tenant = "complhttp-ok";
        Seeded seeded = seed(tenant, InboundFileState.INGESTION_REQUESTED);

        RestAssured.given()
                .header("Authorization", "Bearer good")
                .contentType("application/json")
                .body(envelope(seeded, tenant))
                .post(PATH)
                .then()
                .statusCode(204);

        Assertions.assertEquals(InboundFileState.PARSED, state(seeded));
    }

    @Test
    void missingTokenIs401Problem() throws Exception {
        String tenant = "complhttp-notoken";
        Seeded seeded = seed(tenant, InboundFileState.INGESTION_REQUESTED);

        RestAssured.given()
                .contentType("application/json")
                .body(envelope(seeded, tenant))
                .post(PATH)
                .then()
                .statusCode(401)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("PUSH_TOKEN_REQUIRED"));

        Assertions.assertEquals(InboundFileState.INGESTION_REQUESTED, state(seeded));
    }

    @Test
    void rejectedTokenIs401Problem() throws Exception {
        String tenant = "complhttp-forged";
        Seeded seeded = seed(tenant, InboundFileState.INGESTION_REQUESTED);

        RestAssured.given()
                .header("Authorization", "Bearer forged")
                .contentType("application/json")
                .body(envelope(seeded, tenant))
                .post(PATH)
                .then()
                .statusCode(401)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("PUSH_TOKEN_REJECTED"));

        Assertions.assertEquals(InboundFileState.INGESTION_REQUESTED, state(seeded));
    }

    @Test
    void archivingEntryIs503ProblemForARedelivery() throws Exception {
        String tenant = "complhttp-archiving";
        Seeded seeded = seed(tenant, InboundFileState.ARCHIVING);

        RestAssured.given()
                .header("Authorization", "Bearer good")
                .contentType("application/json")
                .body(envelope(seeded, tenant))
                .post(PATH)
                .then()
                .statusCode(503)
                .contentType(Problem.MEDIA_TYPE)
                .body("code", Matchers.equalTo("ARCHIVING_NOT_COMMITTED"));

        Assertions.assertEquals(InboundFileState.ARCHIVING, state(seeded));
    }

    @Test
    void envelopeWithoutMessageOrDataIs204() {
        RestAssured.given()
                .header("Authorization", "Bearer good")
                .contentType("application/json")
                .body("{\"subscription\":\"projects/p/subscriptions/vendor-ingestion-completions\"}")
                .post(PATH)
                .then()
                .statusCode(204);
        RestAssured.given()
                .header("Authorization", "Bearer good")
                .contentType("application/json")
                .body("{\"message\":{\"messageId\":\"m-empty\"},\"subscription\":\"s\"}")
                .post(PATH)
                .then()
                .statusCode(204);
    }
}
