package com.certifyos.directory_accuracy.clients;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import java.util.List;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** The egress client against WireMock: IAP bearer for the egress audience, tenant header, paths and both 202 shapes. */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockUpstreams.class)
@TestProfile(ApiTestProfile.class)
class EgressClientIT {

    static final String EXPORT = "/api/v1/egress/export";

    @Inject
    @RestClient
    EgressClient egress;

    @InjectMock
    GoogleIdTokenService tokens;

    @BeforeEach
    void before() {
        Mockito.when(tokens.idToken("test-egress-iap-client-id")).thenReturn("egress-id-token");
    }

    private static EgressExportRequest request(String correlationId) {
        return new EgressExportRequest(
                EgressExportRequest.PRACTITIONER,
                "org-a",
                correlationId,
                "t-1",
                "gs://egress-templates/org-a/t-1/v3/mappings.csv",
                ",",
                "csv",
                "[\"locations\"]",
                List.of("1234567893"),
                null,
                EgressClient.INITIATOR,
                new EgressExportRequest.Destination("test-vendor-bucket", "from/org-a/f.csv"));
    }

    @Test
    void exportCarriesTheEgressAudienceTokenAndTenantHeader() {
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(EXPORT))
                .withRequestBody(WireMock.matchingJsonPath("$.correlationId", WireMock.equalTo("c-new")))
                .willReturn(WireMock.aResponse()
                        .withStatus(202)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"type\":\"practitioner\",\"tenant_id\":\"org-a\",\"correlation_id\":\"c-new\","
                                + "\"table\":\"practitioner:org-a:c-new\",\"ttl_days\":7,\"state\":\"PENDING\"}")));

        EgressExportResponse response = egress.export("org-a", request("c-new"));

        Assertions.assertEquals("c-new", response.jobReference().correlationId());
        Assertions.assertEquals("PENDING", response.state());
        WireMockUpstreams.verify(
                1,
                WireMock.postRequestedFor(WireMock.urlEqualTo(EXPORT))
                        .withHeader("Authorization", WireMock.equalTo("Bearer egress-id-token"))
                        .withHeader(EgressClient.TENANT_HEADER, WireMock.equalTo("org-a"))
                        .withHeader("Content-Type", WireMock.containing("application/json"))
                        .withRequestBody(WireMock.matchingJsonPath(
                                "$.destination.bucket", WireMock.equalTo("test-vendor-bucket")))
                        .withRequestBody(WireMock.matchingJsonPath("$.npiFilter[0]", WireMock.equalTo("1234567893"))));
    }

    @Test
    void knownCorrelationIdIsAWrappedJobReference() {
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(EXPORT))
                .withRequestBody(WireMock.matchingJsonPath("$.correlationId", WireMock.equalTo("c-known")))
                .willReturn(WireMock.aResponse()
                        .withStatus(202)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"job\":{\"type\":\"practitioner\",\"tenant_id\":\"org-a\","
                                + "\"correlation_id\":\"c-known\",\"table\":\"t\",\"ttl_days\":7}}")));
        EgressExportResponse response = egress.export("org-a", request("c-known"));
        Assertions.assertEquals("c-known", response.jobReference().correlationId());
        Assertions.assertNull(response.state());
    }

    @Test
    void statusAndCancelUseThePathsAndHeadersEgressExpects() {
        WireMockUpstreams.stubFor(WireMock.get(WireMock.urlEqualTo("/api/v1/egress/status/practitioner/org-a/c-1"))
                .willReturn(WireMock.okJson("{\"state\":\"COMPLETED\",\"phase\":\"EXPORT_DONE\",\"total_rows\":5,"
                        + "\"gcs_uri\":\"gs://b/f.csv\",\"gcs_complete\":true}")));
        WireMockUpstreams.stubFor(
                WireMock.post(WireMock.urlEqualTo("/api/v1/egress/jobs/practitioner/org-a/c-1/cancel"))
                        .willReturn(WireMock.aResponse()
                                .withStatus(202)
                                .withHeader("Content-Type", "application/json")
                                .withBody("{\"status\":\"cancellation_requested\"}")));

        EgressStatusResponse status = egress.status("org-a", "org-a", "c-1");
        EgressCancelResponse cancel = egress.cancel("org-a", "org-a", "c-1");

        Assertions.assertEquals("COMPLETED", status.state());
        Assertions.assertEquals(5L, status.totalRows());
        Assertions.assertEquals("cancellation_requested", cancel.status());
        WireMockUpstreams.verify(
                1,
                WireMock.getRequestedFor(WireMock.urlEqualTo("/api/v1/egress/status/practitioner/org-a/c-1"))
                        .withHeader("Authorization", WireMock.equalTo("Bearer egress-id-token"))
                        .withHeader(EgressClient.TENANT_HEADER, WireMock.equalTo("org-a")));
        WireMockUpstreams.verify(
                1,
                WireMock.postRequestedFor(WireMock.urlEqualTo("/api/v1/egress/jobs/practitioner/org-a/c-1/cancel"))
                        .withHeader("x-user-id", WireMock.equalTo(EgressClient.INITIATOR))
                        .withHeader(EgressClient.TENANT_HEADER, WireMock.equalTo("org-a")));
    }

    @Test
    void conflictSurfacesWithItsStatus() {
        WireMockUpstreams.stubFor(
                WireMock.post(WireMock.urlEqualTo("/api/v1/egress/jobs/practitioner/org-a/c-done/cancel"))
                        .willReturn(WireMock.aResponse().withStatus(409).withBody("{\"error\":\"terminal\"}")));
        WebApplicationException refused =
                Assertions.assertThrows(WebApplicationException.class, () -> egress.cancel("org-a", "org-a", "c-done"));
        Assertions.assertEquals(409, refused.getResponse().getStatus());
    }
}
