package com.certifyos.vendor_exchange.clients;

import com.certifyos.vendor_exchange.ApiTestProfile;
import com.certifyos.vendor_exchange.MongoResource;
import com.certifyos.vendor_exchange.auth.DalUserClient;
import com.certifyos.vendor_exchange.auth.GoogleIdTokenService;
import com.certifyos.vendor_exchange.auth.WireMockDal;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The DAL batch, slice, operational-value and tenant-configuration clients against a WireMock DAL:
 * the IAP bearer, the two headers the DAL interceptor requires, paths and the shapes read back.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockDal.class)
@TestProfile(ApiTestProfile.class)
class DalClientsIT {

    static final String TENANT = "org-a";
    static final String SOURCE_TYPE = "candor:org-a";

    @Inject
    @RestClient
    DalBatchClient batch;

    @Inject
    @RestClient
    DalSliceClient slices;

    @Inject
    @RestClient
    DalOperationalValueClient operationalValues;

    @Inject
    @RestClient
    DalTenantConfigClient tenantConfigs;

    // The real token service would reach for Application Default Credentials, which CI does not have.
    @InjectMock
    GoogleIdTokenService tokens;

    @BeforeEach
    void before() {
        Mockito.when(tokens.idToken("test-dal-iap-client-id")).thenReturn("dal-id-token");
        WireMockDal.resetRequests();
    }

    private static RequestPatternBuilder withDalHeaders(RequestPatternBuilder pattern) {
        return pattern.withHeader("Authorization", WireMock.equalTo("Bearer dal-id-token"))
                .withHeader("requesting-user-id", WireMock.equalTo(DalUserClient.REQUESTING_USER_ID))
                .withHeader(DalUserClient.ORGANIZATION_HEADER, WireMock.equalTo(TENANT));
    }

    @Test
    void batchSendsTransactionAndHeaders() {
        WireMockDal.stubFor(WireMock.post(WireMock.urlEqualTo("/batch"))
                .willReturn(WireMock.okJson(
                        "[{\"alias\":\"practitioner\",\"success\":true,\"result\":{\"id\":\"s-1\",\"note\":null},"
                                + "\"durationMs\":12}]")));
        DalOperation upsert = new DalOperation(
                "CorePractitioner",
                "practitioner",
                "UPSERT",
                Map.of("sourceType", SOURCE_TYPE, "crosswalkId", "cert-1", "data", Map.of("first_name", "Ann")),
                Map.of(),
                List.of());

        List<DalOperationResult> results;
        try (Response response = batch.execute(TENANT, new DalBatchRequest(true, List.of(upsert)))) {
            Assertions.assertEquals(200, response.getStatus());
            results = response.readEntity(new GenericType<List<DalOperationResult>>() {});
        }

        Assertions.assertEquals(1, results.size());
        Assertions.assertTrue(results.get(0).success());
        Assertions.assertEquals("s-1", results.get(0).result().get("id"));
        Assertions.assertTrue(results.get(0).result().containsKey("note"), "a null value survives the copy");
        WireMockDal.verify(
                1,
                withDalHeaders(WireMock.postRequestedFor(WireMock.urlEqualTo("/batch")))
                        .withHeader("Content-Type", WireMock.containing("application/json"))
                        .withRequestBody(
                                WireMock.equalToJson(
                                        """
                                {"transaction": true,
                                 "operations": [{"resource": "CorePractitioner", "alias": "practitioner",
                                   "operation": "UPSERT",
                                   "body": {"sourceType": "candor:org-a", "crosswalkId": "cert-1",
                                            "data": {"first_name": "Ann"}},
                                   "filters": {}, "returning": []}]}
                                """)));
    }

    @Test
    void sliceReadMapsData() {
        String body = "{\"certifyId\":\"cert-1\",\"sourceType\":\"candor:org-a\",\"crosswalkId\":\"%s\","
                + "\"data\":{\"first_name\":\"Ann\",\"middle_name\":null,\"contacts\":[{\"phone\":\"3036790177\"}]},"
                + "\"updatedAt\":\"2026-10-15T12:00:00Z\",\"version\":4}";
        WireMockDal.stubFor(WireMock.get(WireMock.urlEqualTo("/core/practitioner/candor:org-a/cert-1"))
                .willReturn(WireMock.okJson(body.formatted("cert-1"))));
        WireMockDal.stubFor(WireMock.get(WireMock.urlEqualTo("/core/locations/candor:org-a/loc-1"))
                .willReturn(WireMock.okJson(body.formatted("loc-1"))));
        WireMockDal.stubFor(WireMock.get(WireMock.urlEqualTo("/core/entity-addresses/candor:org-a/addr-1"))
                .willReturn(WireMock.okJson(body.formatted("addr-1"))));

        Slice practitioner = slices.practitioner(TENANT, SOURCE_TYPE, "cert-1");
        Slice location = slices.location(TENANT, SOURCE_TYPE, "loc-1");
        Slice address = slices.entityAddress(TENANT, SOURCE_TYPE, "addr-1");

        Assertions.assertEquals("cert-1", practitioner.certifyId());
        Assertions.assertEquals(SOURCE_TYPE, practitioner.sourceType());
        Assertions.assertEquals("Ann", practitioner.data().get("first_name"));
        Assertions.assertTrue(practitioner.data().containsKey("middle_name"), "a null value survives the copy");
        Assertions.assertEquals(
                List.of(Map.of("phone", "3036790177")), practitioner.data().get("contacts"));
        Assertions.assertEquals(Instant.parse("2026-10-15T12:00:00Z"), practitioner.updatedAt());
        Assertions.assertEquals("loc-1", location.crosswalkId());
        Assertions.assertEquals("addr-1", address.crosswalkId());
        WireMockDal.verify(
                1,
                withDalHeaders(
                        WireMock.getRequestedFor(WireMock.urlEqualTo("/core/practitioner/candor:org-a/cert-1"))));
        WireMockDal.verify(
                1, withDalHeaders(WireMock.getRequestedFor(WireMock.urlEqualTo("/core/locations/candor:org-a/loc-1"))));
        WireMockDal.verify(
                1,
                withDalHeaders(
                        WireMock.getRequestedFor(WireMock.urlEqualTo("/core/entity-addresses/candor:org-a/addr-1"))));
    }

    @Test
    void sliceMissingIs404() {
        WireMockDal.stubFor(WireMock.get(WireMock.urlEqualTo("/core/practitioner/candor:org-a/cert-none"))
                .willReturn(WireMock.aResponse().withStatus(404).withBody("{\"error\":\"not found\"}")));

        WebApplicationException missing = Assertions.assertThrows(
                WebApplicationException.class, () -> slices.practitioner(TENANT, SOURCE_TYPE, "cert-none"));

        Assertions.assertEquals(404, missing.getResponse().getStatus());
        // The DAL also answers 404 when a required header is missing; the headers were all there.
        WireMockDal.verify(
                1,
                withDalHeaders(
                        WireMock.getRequestedFor(WireMock.urlEqualTo("/core/practitioner/candor:org-a/cert-none"))));
    }

    @Test
    void practitionerAndLocationOvRead() {
        WireMockDal.stubFor(WireMock.get(WireMock.urlEqualTo("/practitioners/ov/cert-1"))
                .willReturn(WireMock.okJson("{\"certifyId\":\"cert-1\",\"tenantId\":\"org-a\","
                        + "\"data\":{\"first_name\":\"Ann\",\"last_name\":\"Lee\"},"
                        + "\"contributingSlices\":[{\"sourceType\":\"candor:org-a\",\"crosswalkId\":\"cert-1\"}],"
                        + "\"contributionMap\":{\"first_name\":\"candor:org-a\"},\"generatedAt\":\"x\"}")));
        WireMockDal.stubFor(WireMock.get(WireMock.urlEqualTo("/locations/ov/loc-1"))
                .willReturn(WireMock.okJson("{\"certifyId\":\"loc-1\",\"tenantId\":\"org-a\","
                        + "\"data\":{\"address_line_1\":\"1 Main St\"},\"contributingSlices\":[],"
                        + "\"contributionMap\":{}}")));

        OperationalValue practitioner = operationalValues.practitioner(TENANT, "cert-1");
        OperationalValue location = operationalValues.location(TENANT, "loc-1");

        Assertions.assertEquals("cert-1", practitioner.certifyId());
        Assertions.assertEquals("Lee", practitioner.data().get("last_name"));
        Assertions.assertEquals(
                List.of(Map.of("sourceType", SOURCE_TYPE, "crosswalkId", "cert-1")), practitioner.contributingSlices());
        Assertions.assertEquals(Map.of("first_name", SOURCE_TYPE), practitioner.contributionMap());
        Assertions.assertEquals("1 Main St", location.data().get("address_line_1"));
        Assertions.assertEquals(List.of(), location.contributingSlices());
        WireMockDal.verify(
                1, withDalHeaders(WireMock.getRequestedFor(WireMock.urlEqualTo("/practitioners/ov/cert-1"))));
        WireMockDal.verify(1, withDalHeaders(WireMock.getRequestedFor(WireMock.urlEqualTo("/locations/ov/loc-1"))));
    }

    @Test
    void tenantConfigurationsPage() {
        WireMockDal.stubFor(WireMock.get(WireMock.urlPathEqualTo("/tenants/org-a/configurations"))
                .willReturn(WireMock.okJson("{\"data\":[{\"key\":\"source_ranking\",\"value\":[\"candor:org-a\"]}],"
                        + "\"totalCount\":1,\"page\":0}")));

        TenantConfigurationPage page = tenantConfigs.list(TENANT, TENANT, 0, 50);

        Assertions.assertEquals(1L, page.totalCount());
        Assertions.assertEquals("source_ranking", page.data().get(0).get("key"));
        WireMockDal.verify(
                1,
                withDalHeaders(WireMock.getRequestedFor(WireMock.urlPathEqualTo("/tenants/org-a/configurations")))
                        .withQueryParam("page", WireMock.equalTo("0"))
                        .withQueryParam("size", WireMock.equalTo("50")));
    }
}
